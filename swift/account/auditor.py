# Copyright (c) 2010-2012 OpenStack Foundation
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


from swift import gettext_ as _
from swift.account.backend import AccountBroker
from swift.common.exceptions import InvalidAccountInfo
from swift.common.db_auditor import DatabaseAuditor


class AccountAuditor(DatabaseAuditor):
    """Audit accounts."""

    server_type = "account"
    broker_class = AccountBroker

    def _audit(self, info, broker):
        # Validate per policy counts
        policy_stats = broker.get_policy_stats(do_migrations=True)
        policy_totals = {
            'container_count': 0,
            'object_count': 0,
            'bytes_used': 0,
        }
        for policy_stat in policy_stats.values():
            for key in policy_totals:
                policy_totals[key] += policy_stat[key]

        for key in policy_totals:
            if policy_totals[key] == info[key]:
                continue
            return InvalidAccountInfo(_(
                'The total %(key)s for the container (%(total)s) does not '
                'match the sum of %(key)s across policies (%(sum)s)')
                % {'key': key, 'total': info[key], 'sum': policy_totals[key]})
