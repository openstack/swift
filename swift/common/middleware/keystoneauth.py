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
from swift.common.middleware import acl as swift_acl
from swift.common.swob import HTTPNotFound, HTTPForbidden, HTTPUnauthorized


class KeystoneAuth(object):
    """Swift middleware to Keystone authorization system.

    In Swift's proxy-server.conf add this middleware to your pipeline::

        [pipeline:main]
        pipeline = catch_errors cache authtoken keystoneauth proxy-server

    Make sure you have the authtoken middleware before the
    keystoneauth middleware.

    The authtoken middleware will take care of validating the user and
    keystoneauth will authorize access.

    The authtoken middleware is shipped directly with keystone it
    does not have any other dependences than itself so you can either
    install it by copying the file directly in your python path or by
    installing keystone.

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

    This maps tenants to account in Swift.

    The user whose able to give ACL / create Containers permissions
    will be the one that are inside the ``operator_roles``
    setting which by default includes the admin and the swiftoperator
    roles.

    If you need to have a different reseller_prefix to be able to
    mix different auth servers you can configure the option
    ``reseller_prefix`` in your keystoneauth entry like this::

        reseller_prefix = NEWAUTH

    :param app: The next WSGI app in the pipeline
    :param conf: The dict of configuration values
    """
    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.logger = swift_utils.get_logger(conf, log_route='keystoneauth')
        self.reseller_prefix = conf.get('reseller_prefix', 'AUTH_').strip()
        if self.reseller_prefix and self.reseller_prefix[-1] != '_':
            self.reseller_prefix += '_'
        self.operator_roles = conf.get('operator_roles',
                                       'admin, swiftoperator').lower()
        self.reseller_admin_role = conf.get('reseller_admin_role',
                                            'ResellerAdmin').lower()
        config_is_admin = conf.get('is_admin', "false").lower()
        self.is_admin = swift_utils.config_true_value(config_is_admin)
        config_overrides = conf.get('allow_overrides', 't').lower()
        self.allow_overrides = swift_utils.config_true_value(config_overrides)

    def __call__(self, environ, start_response):
        identity = self._keystone_identity(environ)

        # Check if one of the middleware like tempurl or formpost have
        # set the swift.authorize_override environ and want to control the
        # authentication
        if (self.allow_overrides and
                environ.get('swift.authorize_override', False)):
            msg = 'Authorizing from an overriding middleware (i.e: tempurl)'
            self.logger.debug(msg)
            return self.app(environ, start_response)

        if identity:
            self.logger.debug('Using identity: %r' % (identity))
            environ['keystone.identity'] = identity
            environ['REMOTE_USER'] = identity.get('tenant')
            environ['swift.authorize'] = self.authorize
            user_roles = (r.lower() for r in identity.get('roles', []))
            if self.reseller_admin_role in user_roles:
                environ['reseller_request'] = True
        else:
            self.logger.debug('Authorizing as anonymous')
            environ['swift.authorize'] = self.authorize_anonymous

        environ['swift.clean_acl'] = swift_acl.clean_acl

        return self.app(environ, start_response)

    def _keystone_identity(self, environ):
        """Extract the identity from the Keystone auth component."""
        # In next release, we would add user id in env['keystone.identity'] by
        # using _integral_keystone_identity to replace current
        # _keystone_identity. The purpose of keeping it in this release it for
        # back compatibility.
        if environ.get('HTTP_X_IDENTITY_STATUS') != 'Confirmed':
            return
        roles = []
        if 'HTTP_X_ROLES' in environ:
            roles = environ['HTTP_X_ROLES'].split(',')
        identity = {'user': environ.get('HTTP_X_USER_NAME'),
                    'tenant': (environ.get('HTTP_X_TENANT_ID'),
                               environ.get('HTTP_X_TENANT_NAME')),
                    'roles': roles}
        return identity

    def _integral_keystone_identity(self, environ):
        """Extract the identity from the Keystone auth component."""
        if environ.get('HTTP_X_IDENTITY_STATUS') != 'Confirmed':
            return
        roles = []
        if 'HTTP_X_ROLES' in environ:
            roles = environ['HTTP_X_ROLES'].split(',')
        identity = {'user': (environ.get('HTTP_X_USER_ID'),
                             environ.get('HTTP_X_USER_NAME')),
                    'tenant': (environ.get('HTTP_X_TENANT_ID'),
                               environ.get('HTTP_X_TENANT_NAME')),
                    'roles': roles}
        return identity

    def _get_account_for_tenant(self, tenant_id):
        return '%s%s' % (self.reseller_prefix, tenant_id)

    def _reseller_check(self, account, tenant_id):
        """Check reseller prefix."""
        return account == self._get_account_for_tenant(tenant_id)

    def _authorize_cross_tenant(self, user_id, user_name,
                                tenant_id, tenant_name, roles):
        """Check cross-tenant ACLs.

        Match tenant:user, tenant and user could be its id, name or '*'

        :param user_id: The user id from the identity token.
        :param user_name: The user name from the identity token.
        :param tenant_id: The tenant ID from the identity token.
        :param tenant_name: The tenant name from the identity token.
        :param roles: The given container ACL.

        :returns: matched string if tenant(name/id/*):user(name/id/*) matches
                  the given ACL.
                  None otherwise.

        """
        for tenant in [tenant_id, tenant_name, '*']:
            for user in [user_id, user_name, '*']:
                s = '%s:%s' % (tenant, user)
                if s in roles:
                    return s
        return None

    def authorize(self, req):
        env = req.environ
        env_identity = self._integral_keystone_identity(env)
        tenant_id, tenant_name = env_identity['tenant']
        user_id, user_name = env_identity['user']
        referrers, roles = swift_acl.parse_acl(getattr(req, 'acl', None))

        #allow OPTIONS requests to proceed as normal
        if req.method == 'OPTIONS':
            return

        try:
            part = req.split_path(1, 4, True)
            version, account, container, obj = part
        except ValueError:
            return HTTPNotFound(request=req)

        user_roles = [r.lower() for r in env_identity.get('roles', [])]

        # Give unconditional access to a user with the reseller_admin
        # role.
        if self.reseller_admin_role in user_roles:
            msg = 'User %s has reseller admin authorizing'
            self.logger.debug(msg % tenant_id)
            req.environ['swift_owner'] = True
            return

        # If we are not reseller admin and user is trying to delete its own
        # account then deny it.
        if not container and not obj and req.method == 'DELETE':
            # User is not allowed to issue a DELETE on its own account
            msg = 'User %s:%s is not allowed to delete its own account'
            self.logger.debug(msg % (tenant_name, user_name))
            return self.denied_response(req)

        # cross-tenant authorization
        matched_acl = self._authorize_cross_tenant(user_id, user_name,
                                                   tenant_id, tenant_name,
                                                   roles)
        if matched_acl is not None:
            log_msg = 'user %s allowed in ACL authorizing.' % matched_acl
            self.logger.debug(log_msg)
            return

        acl_authorized = self._authorize_unconfirmed_identity(req, obj,
                                                              referrers,
                                                              roles)
        if acl_authorized:
            return

        # Check if a user tries to access an account that does not match their
        # token
        if not self._reseller_check(account, tenant_id):
            log_msg = 'tenant mismatch: %s != %s' % (account, tenant_id)
            self.logger.debug(log_msg)
            return self.denied_response(req)

        # Check the roles the user is belonging to. If the user is
        # part of the role defined in the config variable
        # operator_roles (like admin) then it will be
        # promoted as an admin of the account/tenant.
        for role in self.operator_roles.split(','):
            role = role.strip()
            if role in user_roles:
                log_msg = 'allow user with role %s as account admin' % (role)
                self.logger.debug(log_msg)
                req.environ['swift_owner'] = True
                return

        # If user is of the same name of the tenant then make owner of it.
        if self.is_admin and user_name == tenant_name:
            self.logger.warning("the is_admin feature has been deprecated "
                                "and will be removed in the future "
                                "update your config file")
            req.environ['swift_owner'] = True
            return

        if acl_authorized is not None:
            return self.denied_response(req)

        # Check if we have the role in the userroles and allow it
        for user_role in user_roles:
            if user_role in (r.lower() for r in roles):
                log_msg = 'user %s:%s allowed in ACL: %s authorizing'
                self.logger.debug(log_msg % (tenant_name, user_name,
                                             user_role))
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

        #allow OPTIONS requests to proceed as normal
        if req.method == 'OPTIONS':
            return

        is_authoritative_authz = (account and
                                  account.startswith(self.reseller_prefix))
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
            log_msg = 'allowing proxy %s for container-sync' % req.remote_addr
            self.logger.debug(log_msg)
            return True

        # Check if referrer is allowed.
        if swift_acl.referrer_allowed(req.referer, referrers):
            if obj or '.rlistings' in roles:
                log_msg = 'authorizing %s via referer ACL' % req.referrer
                self.logger.debug(log_msg)
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
