# Copyright (c) 2010 OpenStack, LLC.
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

from swift.common.internal_proxy import InternalProxy
from swift.common.exceptions import ChunkReadTimeout
from swift.common.utils import get_logger

class ConfigError(Exception):
    pass

class MissingProxyConfig(ConfigError):
    pass

class LogProcessor(object):

    def __init__(self, conf, logger):
        stats_conf = conf.get('log-processor', {})
        
        working_dir = stats_conf.get('working_dir', '/tmp/swift/')
        if working_dir.endswith('/') and len(working_dir) > 1:
            working_dir = working_dir[:-1]
        self.working_dir = working_dir
        proxy_server_conf_loc = stats_conf.get('proxy_server_conf',
                                               '/etc/swift/proxy-server.conf')
        try:
            c = ConfigParser()
            c.read(proxy_server_conf_loc)
            proxy_server_conf = dict(c.items('proxy-server'))
        except:
            raise MissingProxyConfig()
        self.proxy_server_conf = proxy_server_conf
        if isinstance(logger, tuple):
            self.logger = get_logger(*logger)
        else:
            self.logger = logger
        self.internal_proxy = InternalProxy(self.proxy_server_conf,
                                            self.logger,
                                            retries=3)
        
        # load the processing plugins
        self.plugins = {}
        plugin_prefix = 'log-processor-'
        for section in (x for x in conf if x.startswith(plugin_prefix)):
            plugin_name = section[len(plugin_prefix):]
            plugin_conf = conf.get(section, {})
            self.plugins[plugin_name] = plugin_conf
            import_target, class_name = plugin_conf['class_path'].rsplit('.', 1)
            module = __import__(import_target, fromlist=[import_target])
            klass = getattr(module, class_name)
            self.plugins[plugin_name]['instance'] = klass(plugin_conf)

    def process_one_file(self, plugin_name, account, container, object_name):
        # get an iter of the object data
        compressed = object_name.endswith('.gz')
        stream = self.get_object_data(account, container, object_name,
                                      compressed=compressed)
        # look up the correct plugin and send the stream to it
        return self.plugins[plugin_name]['instance'].process(stream)

    def get_data_list(self, start_date=None, end_date=None, listing_filter=None):
        total_list = []
        for p in self.plugins:
            account = p['swift_account']
            container = p['container_name']
            l = self.get_container_listing(account, container, start_date,
                                           end_date, listing_filter)
            for i in l:
                total_list.append((p, account, container, i))
        return total_list

    def get_container_listing(self, swift_account, container_name, start_date=None,
                         end_date=None, listing_filter=None):
        '''
        Get a container listing, filtered by start_date, end_date, and
        listing_filter. Dates, if given, should be in YYYYMMDDHH format
        '''
        search_key = None
        if start_date is not None:
            date_parts = []
            try:
                year, start_date = start_date[:4], start_date[4:]
                if year:
                    date_parts.append(year)
                    month, start_date = start_date[:2], start_date[2:]
                    if month:
                        date_parts.append(month)
                        day, start_date = start_date[:2], start_date[2:]
                        if day:
                            date_parts.append(day)
                            hour, start_date = start_date[:2], start_date[2:]
                            if hour:
                                date_parts.append(hour)
            except IndexError:
                pass
            else:
                search_key = '/'.join(date_parts)
        end_key = None
        if end_date is not None:
            date_parts = []
            try:
                year, end_date = end_date[:4], end_date[4:]
                if year:
                    date_parts.append(year)
                    month, end_date = end_date[:2], end_date[2:]
                    if month:
                        date_parts.append(month)
                        day, end_date = end_date[:2], end_date[2:]
                        if day:
                            date_parts.append(day)
                            hour, end_date = end_date[:2], end_date[2:]
                            if hour:
                                date_parts.append(hour)
            except IndexError:
                pass
            else:
                end_key = '/'.join(date_parts)
        container_listing = self.internal_proxy.get_container_list(
                                    swift_account,
                                    container_name,
                                    marker=search_key)
        results = []
        if container_listing is not None:
            if listing_filter is None:
                listing_filter = set()
            for item in container_listing:
                name = item['name']
                if end_key and name > end_key:
                    break
                if name not in listing_filter:
                    results.append(name)
        return results

    def get_object_data(self, swift_account, container_name, object_name,
                        compressed=False):
        '''reads an object and yields its lines'''
        o = self.internal_proxy.get_object(swift_account,
                                           container_name,
                                           object_name)
        last_part = ''
        last_compressed_part = ''
        # magic in the following zlib.decompressobj argument is courtesy of
        # http://stackoverflow.com/questions/2423866/python-decompressing-gzip-chunk-by-chunk
        d = zlib.decompressobj(16+zlib.MAX_WBITS)
        try:
            for chunk in o:
                if compressed:
                    try:
                        chunk = d.decompress(chunk)
                    except zlib.error:
                        raise BadFileDownload() # bad compressed data
                parts = chunk.split('\n')
                parts[0] = last_part + parts[0]
                for part in parts[:-1]:
                    yield part
                last_part = parts[-1]
            if last_part:
                yield last_part
        except ChunkReadTimeout:
            raise BadFileDownload()

def multiprocess_collate(processor_args,
                         start_date=None,
                         end_date=None,
                         listing_filter=None):
    '''get listing of files and yield hourly data from them'''
    p = LogProcessor(*processor_args)
    all_files = p.get_data_list(start_date, end_date, listing_filter)

    p.logger.info('loaded %d files to process' % len(all_files))

    if not all_files:
        # no work to do
        return

    worker_count = multiprocessing.cpu_count() - 1
    results = []
    in_queue = multiprocessing.Queue()
    out_queue = multiprocessing.Queue()
    for _ in range(worker_count):
        p = multiprocessing.Process(target=collate_worker,
                                    args=(processor_args,
                                          in_queue,
                                          out_queue))
        p.start()
        results.append(p)
    for x in all_files:
        in_queue.put(x)
    for _ in range(worker_count):
        in_queue.put(None)
    count = 0
    while True:
        try:
            item, data = out_queue.get_nowait()
            count += 1
            if data:
                yield item, data
            if count >= len(all_files):
                # this implies that one result will come from every request
                break
        except Queue.Empty:
            time.sleep(.1)
    for r in results:
        r.join()

def collate_worker(processor_args, in_queue, out_queue):
    '''worker process for multiprocess_collate'''
    p = LogProcessor(*processor_args)
    while True:
        try:
            item = in_queue.get_nowait()
            if item is None:
                break
        except Queue.Empty:
            time.sleep(.1)
        else:
            ret = None
            ret = p.process_one_file(item)
            out_queue.put((item, ret))