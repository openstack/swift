# Copyright (c) 2012 OpenStack, LLC.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.
'''
Created on February 27, 2012

A filter that disallows any paths that contain defined forbidden characters
or that exceed a defined length.

Place in proxy filter before proxy, e.g.

[pipeline:main]
pipeline = catch_errors healthcheck name_check cache tempauth sos proxy-server

[filter:name_check]
use = egg:swift#name_check
forbidden_chars = '"`<>
maximum_length = 255

There are default settings for forbidden_chars (FORBIDDEN_CHARS) and
maximum_length (MAX_LENGTH)

The filter returns HTTPBadRequest if path is invalid.

@author: eamonn-otoole
'''

from swift.common.utils import get_logger
from webob import Request
from webob.exc import HTTPBadRequest
from urllib2 import unquote

FORBIDDEN_CHARS = "\'\"`<>"
MAX_LENGTH = 255


class NameCheckMiddleware(object):

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.forbidden_chars = self.conf.get('forbidden_chars',
                                            FORBIDDEN_CHARS)
        self.maximum_length = self.conf.get('maximum_length', MAX_LENGTH)
        self.logger = get_logger(self.conf, log_route='name_check')

    def check_character(self, req):
        '''
        Checks req.path for any forbidden characters
        Returns True if there are any forbidden characters
        Returns False if there aren't any forbidden characters
        '''
        self.logger.debug("name_check: path %s" % req.path)
        self.logger.debug("name_check: self.forbidden_chars %s" %
                           self.forbidden_chars)

        for c in unquote(req.path):
            if c in self.forbidden_chars:
                return True
            else:
                pass
        return False

    def check_length(self, req):
        '''
        Checks that req.path doesn't exceed the defined maximum length
        Returns True if the length exceeds the maximum
        Returns False if the length is <= the maximum
        '''
        length = len(unquote(req.path))
        if length > self.maximum_length:
            return True
        else:
            return False

    def __call__(self, env, start_response):
        req = Request(env)

        if self.check_character(req):
            return HTTPBadRequest(request=req,
               body=("Object/Container name contains forbidden chars from %s"
               % self.forbidden_chars))(env, start_response)
        elif self.check_length(req):
            return HTTPBadRequest(request=req,
               body=("Object/Container name longer than the allowed maximum %s"
               % self.maximum_length))(env, start_response)
        else:
            # Pass on to downstream WSGI component
            return self.app(env, start_response)


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def name_check_filter(app):
        return NameCheckMiddleware(app, conf)
    return name_check_filter
