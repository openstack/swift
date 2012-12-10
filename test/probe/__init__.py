from test import get_config
config = get_config('probe_test')
CHECK_SERVER_TIMEOUT = int(config.get('check_server_timeout', 30))
