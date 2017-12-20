# Copyright 2012 OpenStack Foundation
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

from swift.common import utils as swift_utils
from swift.common.http import is_success
from swift.common.middleware import acl as swift_acl
from swift.common.request_helpers import get_sys_meta_prefix
from swift.common.swob import HTTPNotFound, HTTPForbidden, HTTPUnauthorized
from swift.common.utils import config_read_reseller_options, list_from_csv
from swift.proxy.controllers.base import get_account_info
import functools

PROJECT_DOMAIN_ID_HEADER = 'x-account-project-domain-id'
PROJECT_DOMAIN_ID_SYSMETA_HEADER = \
    get_sys_meta_prefix('account') + 'project-domain-id'
# a string that is unique w.r.t valid ids
UNKNOWN_ID = '_unknown'


class KeystoneAuth(object):
    """Swift middleware to Keystone authorization system.

    In Swift's proxy-server.conf add this keystoneauth middleware and the
    authtoken middleware to your pipeline. Make sure you have the authtoken
    middleware before the keystoneauth middleware.

    The authtoken middleware will take care of validating the user and
    keystoneauth will authorize access.

    The sample proxy-server.conf shows a sample pipeline that uses keystone.

    :download:`proxy-server.conf-sample </../../etc/proxy-server.conf-sample>`

    The authtoken middleware is shipped with keystonemiddleware - it
    does not have any other dependencies than itself so you can either
    install it by copying the file directly in your python path or by
    installing keystonemiddleware.

    If support is required for unvalidated users (as with anonymous
    access) or for formpost/staticweb/tempurl middleware, authtoken will
    need to be configured with ``delay_auth_decision`` set to true.  See
    the Keystone documentation for more detail on how to configure the
    authtoken middleware.

    In proxy-server.conf you will need to have the setting account
    auto creation to true::

        [app:proxy-server]
        account_autocreate = true

    And add a swift authorization filter section, such as::

        [filter:keystoneauth]
        use = egg:swift#keystoneauth
        operator_roles = admin, swiftoperator

    The user who is able to give ACL / create Containers permissions
    will be the user with a role listed in the ``operator_roles``
    setting which by default includes the admin and the swiftoperator
    roles.

    The keystoneauth middleware maps a Keystone project/tenant to an account
    in Swift by adding a prefix (``AUTH_`` by default) to the tenant/project
    id.. For example, if the project id is ``1234``, the path is
    ``/v1/AUTH_1234``.

    If you need to have a different reseller_prefix to be able to
    mix different auth servers you can configure the option
    ``reseller_prefix`` in your keystoneauth entry like this::

        reseller_prefix = NEWAUTH

    Don't forget to also update the Keystone service endpoint configuration to
    use NEWAUTH in the path.

    It is possible to have several accounts associated with the same project.
    This is done by listing several prefixes as shown in the following
    example::

        reseller_prefix = AUTH, SERVICE

    This means that for project id '1234', the paths '/v1/AUTH_1234' and
    '/v1/SERVICE_1234' are associated with the project and are authorized
    using roles that a user has with that project. The core use of this feature
    is that it is possible to provide different rules for each account
    prefix. The following parameters may be prefixed with the appropriate
    prefix::

        operator_roles
        service_roles

    For backward compatibility, if either of these parameters is specified
    without a prefix then it applies to all reseller_prefixes. Here is an
    example, using two prefixes::

        reseller_prefix = AUTH, SERVICE
        # The next three lines have identical effects (since the first applies
        # to both prefixes).
        operator_roles = admin, swiftoperator
        AUTH_operator_roles = admin, swiftoperator
        SERVICE_operator_roles = admin, swiftoperator
        # The next line only applies to accounts with the SERVICE prefix
        SERVICE_operator_roles = admin, some_other_role

    X-Service-Token tokens are supported by the inclusion of the service_roles
    configuration option. When present, this option requires that the
    X-Service-Token header supply a token from a user who has a role listed
    in service_roles. Here is an example configuration::

        reseller_prefix = AUTH, SERVICE
        AUTH_operator_roles = admin, swiftoperator
        SERVICE_operator_roles = admin, swiftoperator
        SERVICE_service_roles = service

    The keystoneauth middleware supports cross-tenant access control using the
    syntax ``<tenant>:<user>`` to specify a grantee in container Access Control
    Lists (ACLs). For a request to be granted by an ACL, the grantee
    ``<tenant>`` must match the UUID of the tenant to which the request
    X-Auth-Token is scoped and the grantee ``<user>`` must match the UUID of
    the user authenticated by that token.

    Note that names must no longer be used in cross-tenant ACLs because with
    the introduction of domains in keystone names are no longer globally
    unique.

    For backwards compatibility, ACLs using names will be granted by
    keystoneauth when it can be established that the grantee tenant,
    the grantee user and the tenant being accessed are either not yet in a
    domain (e.g. the X-Auth-Token has been obtained via the keystone v2
    API) or are all in the default domain to which legacy accounts would
    have been migrated. The default domain is identified by its UUID,
    which by default has the value ``default``. This can be changed by
    setting the ``default_domain_id`` option in the keystoneauth
    configuration::

        default_domain_id = default

    The backwards compatible behavior can be disabled by setting the config
    option ``allow_names_in_acls`` to false::

        allow_names_in_acls = false

    To enable this backwards compatibility, keystoneauth will attempt to
    determine the domain id of a tenant when any new account is created,
    and persist this as account metadata. If an account is created for a tenant
    using a token with reselleradmin role that is not scoped on that tenant,
    keystoneauth is unable to determine the domain id of the tenant;
    keystoneauth will assume that the tenant may not be in the default domain
    and therefore not match names in ACLs for that account.

    By default, middleware higher in the WSGI pipeline may override auth
    processing, useful for middleware such as tempurl and formpost. If you know
    you're not going to use such middleware and you want a bit of extra
    security you can disable this behaviour by setting the ``allow_overrides``
    option to ``false``::

        allow_overrides = false

    :param app: The next WSGI app in the pipeline
    :param conf: The dict of configuration values
    """
    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = swift_utils.get_logger(conf, log_route='keystoneauth')
        self.reseller_prefixes, self.account_rules = \
            config_read_reseller_options(conf,
                                         dict(operator_roles=['admin',
                                                              'swiftoperator'],
                                              service_roles=[]))
        self.reseller_admin_role = conf.get('reseller_admin_role',
                                            'ResellerAdmin').lower()
        config_is_admin = conf.get('is_admin', "false").lower()
        if swift_utils.config_true_value(config_is_admin):
            self.logger.warning("The 'is_admin' option for keystoneauth is no "
                                "longer supported. Remove the 'is_admin' "
                                "option from your keystoneauth config")

        config_overrides = conf.get('allow_overrides', 't').lower()
        self.allow_overrides = swift_utils.config_true_value(config_overrides)
        self.default_domain_id = conf.get('default_domain_id', 'default')
        self.allow_names_in_acls = swift_utils.config_true_value(
            conf.get('allow_names_in_acls', 'true'))

    def __call__(self, environ, start_response):
        env_identity = self._keystone_identity(environ)

        # Check if one of the middleware like tempurl or formpost have
        # set the swift.authorize_override environ and want to control the
        # authentication
        if (self.allow_overrides and
                environ.get('swift.authorize_override', False)):
            msg = 'Authorizing from an overriding middleware'
            self.logger.debug(msg)
            return self.app(environ, start_response)

        if env_identity:
            self.logger.debug('Using identity: %r', env_identity)
            environ['REMOTE_USER'] = env_identity.get('tenant')
            environ['keystone.identity'] = env_identity
            environ['swift.authorize'] = functools.partial(
                self.authorize, env_identity)
            user_roles = (r.lower() for r in env_identity.get('roles', []))
            if self.reseller_admin_role in user_roles:
                environ['reseller_request'] = True
        else:
            self.logger.debug('Authorizing as anonymous')
            environ['swift.authorize'] = self.authorize_anonymous

        environ['swift.clean_acl'] = swift_acl.clean_acl

        def keystone_start_response(status, response_headers, exc_info=None):
            project_domain_id = None
            for key, val in response_headers:
                if key.lower() == PROJECT_DOMAIN_ID_SYSMETA_HEADER:
                    project_domain_id = val
                    break
            if project_domain_id:
                response_headers.append((PROJECT_DOMAIN_ID_HEADER,
                                         project_domain_id))
            return start_response(status, response_headers, exc_info)

        return self.app(environ, keystone_start_response)

    def _keystone_identity(self, environ):
        """Extract the identity from the Keystone auth component."""
        if (environ.get('HTTP_X_IDENTITY_STATUS') != 'Confirmed'
            or environ.get(
                'HTTP_X_SERVICE_IDENTITY_STATUS') not in (None, 'Confirmed')):
            return
        roles = list_from_csv(environ.get('HTTP_X_ROLES', ''))
        service_roles = list_from_csv(environ.get('HTTP_X_SERVICE_ROLES', ''))
        identity = {'user': (environ.get('HTTP_X_USER_ID'),
                             environ.get('HTTP_X_USER_NAME')),
                    'tenant': (environ.get('HTTP_X_PROJECT_ID',
                                           environ.get('HTTP_X_TENANT_ID')),
                               environ.get('HTTP_X_PROJECT_NAME',
                                           environ.get('HTTP_X_TENANT_NAME'))),
                    'roles': roles,
                    'service_roles': service_roles}
        token_info = environ.get('keystone.token_info', {})
        auth_version = 0
        user_domain = project_domain = (None, None)
        if 'access' in token_info:
            # ignore any domain id headers that authtoken may have set
            auth_version = 2
        elif 'token' in token_info:
            auth_version = 3
            user_domain = (environ.get('HTTP_X_USER_DOMAIN_ID'),
                           environ.get('HTTP_X_USER_DOMAIN_NAME'))
            project_domain = (environ.get('HTTP_X_PROJECT_DOMAIN_ID'),
                              environ.get('HTTP_X_PROJECT_DOMAIN_NAME'))
        identity['user_domain'] = user_domain
        identity['project_domain'] = project_domain
        identity['auth_version'] = auth_version
        return identity

    def _get_account_name(self, prefix, tenant_id):
        return '%s%s' % (prefix, tenant_id)

    def _account_matches_tenant(self, account, tenant_id):
        """Check if account belongs to a project/tenant"""
        for prefix in self.reseller_prefixes:
            if self._get_account_name(prefix, tenant_id) == account:
                return True
        return False

    def _get_account_prefix(self, account):
        """Get the prefix of an account"""
        # Empty prefix matches everything, so try to match others first
        for prefix in [pre for pre in self.reseller_prefixes if pre != '']:
            if account.startswith(prefix):
                return prefix
        if '' in self.reseller_prefixes:
            return ''
        return None

    def _get_project_domain_id(self, environ):
        info = get_account_info(environ, self.app, 'KS')
        domain_id = info.get('sysmeta', {}).get('project-domain-id')
        exists = (is_success(info.get('status', 0))
                  and info.get('account_really_exists', True))
        return exists, domain_id

    def _set_project_domain_id(self, req, path_parts, env_identity):
        '''
        Try to determine the project domain id and save it as
        account metadata. Do this for a PUT or POST to the
        account, and also for a container PUT in case that
        causes the account to be auto-created.
        '''
        if PROJECT_DOMAIN_ID_SYSMETA_HEADER in req.headers:
            return

        version, account, container, obj = path_parts
        method = req.method
        if (obj or (container and method != 'PUT')
                or method not in ['PUT', 'POST']):
            return

        tenant_id, tenant_name = env_identity['tenant']
        exists, sysmeta_id = self._get_project_domain_id(req.environ)
        req_has_id, req_id, new_id = False, None, None
        if self._account_matches_tenant(account, tenant_id):
            # domain id can be inferred from request (may be None)
            req_has_id = True
            req_id = env_identity['project_domain'][0]
        if not exists:
            # new account so set a domain id
            new_id = req_id if req_has_id else UNKNOWN_ID
        elif sysmeta_id is None and req_id == self.default_domain_id:
            # legacy account, update if default domain id in req
            new_id = req_id
        elif sysmeta_id == UNKNOWN_ID and req_has_id:
            # unknown domain, update if req confirms domain
            new_id = req_id or ''
        elif req_has_id and sysmeta_id != req_id:
            self.logger.warning("Inconsistent project domain id: " +
                                "%s in token vs %s in account metadata."
                                % (req_id, sysmeta_id))

        if new_id is not None:
            req.headers[PROJECT_DOMAIN_ID_SYSMETA_HEADER] = new_id

    def _is_name_allowed_in_acl(self, req, path_parts, identity):
        if not self.allow_names_in_acls:
            return False
        user_domain_id = identity['user_domain'][0]
        if user_domain_id and user_domain_id != self.default_domain_id:
            return False

        proj_domain_id = identity['project_domain'][0]
        if proj_domain_id and proj_domain_id != self.default_domain_id:
            return False

        # request user and scoped project are both in default domain
        tenant_id, tenant_name = identity['tenant']
        version, account, container, obj = path_parts
        if self._account_matches_tenant(account, tenant_id):
            # account == scoped project, so account is also in default domain
            allow = True
        else:
            # retrieve account project domain id from account sysmeta
            exists, acc_domain_id = self._get_project_domain_id(req.environ)
            allow = exists and acc_domain_id in [self.default_domain_id, None]
        if allow:
            self.logger.debug("Names allowed in acls.")
        return allow

    def _authorize_cross_tenant(self, user_id, user_name,
                                tenant_id, tenant_name, roles,
                                allow_names=True):
        """Check cross-tenant ACLs.

        Match tenant:user, tenant and user could be its id, name or '*'

        :param user_id: The user id from the identity token.
        :param user_name: The user name from the identity token.
        :param tenant_id: The tenant ID from the identity token.
        :param tenant_name: The tenant name from the identity token.
        :param roles: The given container ACL.
        :param allow_names: If True then attempt to match tenant and user names
                            as well as id's.

        :returns: matched string if tenant(name/id/*):user(name/id/*) matches
                  the given ACL.
                  None otherwise.

        """
        tenant_match = [tenant_id, '*']
        user_match = [user_id, '*']
        if allow_names:
            tenant_match = tenant_match + [tenant_name]
            user_match = user_match + [user_name]
        for tenant in tenant_match:
            for user in user_match:
                s = '%s:%s' % (tenant, user)
                if s in roles:
                    return s
        return None

    def authorize(self, env_identity, req):
        # Cleanup - make sure that a previously set swift_owner setting is
        # cleared now. This might happen for example with COPY requests.
        req.environ.pop('swift_owner', None)

        tenant_id, tenant_name = env_identity['tenant']
        user_id, user_name = env_identity['user']
        referrers, roles = swift_acl.parse_acl(getattr(req, 'acl', None))

        # allow OPTIONS requests to proceed as normal
        if req.method == 'OPTIONS':
            return

        try:
            part = req.split_path(1, 4, True)
            version, account, container, obj = part
        except ValueError:
            return HTTPNotFound(request=req)

        self._set_project_domain_id(req, part, env_identity)

        user_roles = [r.lower() for r in env_identity.get('roles', [])]
        user_service_roles = [r.lower() for r in env_identity.get(
                              'service_roles', [])]

        # Give unconditional access to a user with the reseller_admin
        # role.
        if self.reseller_admin_role in user_roles:
            msg = 'User %s has reseller admin authorizing'
            self.logger.debug(msg, tenant_id)
            req.environ['swift_owner'] = True
            return

        # If we are not reseller admin and user is trying to delete its own
        # account then deny it.
        if not container and not obj and req.method == 'DELETE':
            # User is not allowed to issue a DELETE on its own account
            msg = 'User %s:%s is not allowed to delete its own account'
            self.logger.debug(msg, tenant_name, user_name)
            return self.denied_response(req)

        # cross-tenant authorization
        matched_acl = None
        if roles:
            allow_names = self._is_name_allowed_in_acl(req, part, env_identity)
            matched_acl = self._authorize_cross_tenant(user_id, user_name,
                                                       tenant_id, tenant_name,
                                                       roles, allow_names)
        if matched_acl is not None:
            log_msg = 'user %s allowed in ACL authorizing.'
            self.logger.debug(log_msg, matched_acl)
            return

        acl_authorized = self._authorize_unconfirmed_identity(req, obj,
                                                              referrers,
                                                              roles)
        if acl_authorized:
            return

        # Check if a user tries to access an account that does not match their
        # token
        if not self._account_matches_tenant(account, tenant_id):
            log_msg = 'tenant mismatch: %s != %s'
            self.logger.debug(log_msg, account, tenant_id)
            return self.denied_response(req)

        # Compare roles from tokens against the configuration options:
        #
        # X-Auth-Token role  Has specified  X-Service-Token role  Grant
        # in operator_roles? service_roles? in service_roles?     swift_owner?
        # ------------------ -------------- --------------------  ------------
        # yes                yes            yes                   yes
        # yes                yes            no                    no
        # yes                no             don't care            yes
        # no                 don't care     don't care            no
        # ------------------ -------------- --------------------  ------------
        account_prefix = self._get_account_prefix(account)
        operator_roles = self.account_rules[account_prefix]['operator_roles']
        have_operator_role = set(operator_roles).intersection(
            set(user_roles))
        service_roles = self.account_rules[account_prefix]['service_roles']
        have_service_role = set(service_roles).intersection(
            set(user_service_roles))
        allowed = False
        if have_operator_role and (service_roles and have_service_role):
            allowed = True
        elif have_operator_role and not service_roles:
            allowed = True
        if allowed:
            log_msg = 'allow user with role(s) %s as account admin'
            self.logger.debug(log_msg, ','.join(have_operator_role.union(
                                                have_service_role)))
            req.environ['swift_owner'] = True
            return

        if acl_authorized is not None:
            return self.denied_response(req)

        # Check if we have the role in the userroles and allow it
        for user_role in user_roles:
            if user_role in (r.lower() for r in roles):
                log_msg = 'user %s:%s allowed in ACL: %s authorizing'
                self.logger.debug(log_msg, tenant_name, user_name,
                                  user_role)
                return

        return self.denied_response(req)

    def authorize_anonymous(self, req):
        """
        Authorize an anonymous request.

        :returns: None if authorization is granted, an error page otherwise.
        """
        try:
            part = req.split_path(1, 4, True)
            version, account, container, obj = part
        except ValueError:
            return HTTPNotFound(request=req)

        # allow OPTIONS requests to proceed as normal
        if req.method == 'OPTIONS':
            return

        is_authoritative_authz = (account and
                                  (self._get_account_prefix(account) in
                                   self.reseller_prefixes))
        if not is_authoritative_authz:
            return self.denied_response(req)

        referrers, roles = swift_acl.parse_acl(getattr(req, 'acl', None))
        authorized = self._authorize_unconfirmed_identity(req, obj, referrers,
                                                          roles)
        if not authorized:
            return self.denied_response(req)

    def _authorize_unconfirmed_identity(self, req, obj, referrers, roles):
        """"
        Perform authorization for access that does not require a
        confirmed identity.

        :returns: A boolean if authorization is granted or denied.  None if
                  a determination could not be made.
        """
        # Allow container sync.
        if (req.environ.get('swift_sync_key')
                and (req.environ['swift_sync_key'] ==
                     req.headers.get('x-container-sync-key', None))
                and 'x-timestamp' in req.headers):
            log_msg = 'allowing proxy %s for container-sync'
            self.logger.debug(log_msg, req.remote_addr)
            return True

        # Check if referrer is allowed.
        if swift_acl.referrer_allowed(req.referer, referrers):
            if obj or '.rlistings' in roles:
                log_msg = 'authorizing %s via referer ACL'
                self.logger.debug(log_msg, req.referrer)
                return True
            return False

    def denied_response(self, req):
        """Deny WSGI Response.

        Returns a standard WSGI response callable with the status of 403 or 401
        depending on whether the REMOTE_USER is set or not.
        """
        if req.remote_user:
            return HTTPForbidden(request=req)
        else:
            return HTTPUnauthorized(request=req)


def filter_factory(global_conf, **local_conf):
    """Returns a WSGI filter app for use with paste.deploy."""
    conf = global_conf.copy()
    conf.update(local_conf)

    def auth_filter(app):
        return KeystoneAuth(app, conf)
    return auth_filter
