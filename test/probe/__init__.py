from test import get_config
from swift.common.utils import config_true_value
config = get_config('probe_test')
CHECK_SERVER_TIMEOUT = int(config.get('check_server_timeout', 30))
VALIDATE_RSYNC = config_true_value(config.get('validate_rsync', False))
