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

from ConfigParser import ConfigParser
import zlib
import time
import datetime
import cStringIO
import collections
from paste.deploy import appconfig
import multiprocessing
import Queue
import cPickle

from swift.common.internal_proxy import InternalProxy
from swift.common.exceptions import ChunkReadTimeout
from swift.common.utils import get_logger, readconf
from swift.common.daemon import Daemon

class BadFileDownload(Exception):
    pass

class LogProcessor(object):

    def __init__(self, conf, logger):
        stats_conf = conf.get('log-processor', {})
        
        proxy_server_conf_loc = stats_conf.get('proxy_server_conf',
                                               '/etc/swift/proxy-server.conf')
        self.proxy_server_conf = appconfig('config:%s' % proxy_server_conf_loc,
                                            name='proxy-server')
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
        return self.plugins[plugin_name]['instance'].process(stream,
                                                             account,
                                                             container,
                                                             object_name)

    def get_data_list(self, start_date=None, end_date=None, listing_filter=None):
        total_list = []
        for name, data in self.plugins.items():
            account = data['swift_account']
            container = data['container_name']
            l = self.get_container_listing(account, container, start_date,
                                           end_date, listing_filter)
            for i in l:
                # The items in this list end up being passed as positional
                # parameters to process_one_file.
                total_list.append((name, account, container, i))
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
        code, o = self.internal_proxy.get_object(swift_account,
                                           container_name,
                                           object_name)
        if code < 200 or code >= 300:
            return
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

    def generate_keylist_mapping(self):
        keylist = {}
        for plugin in self.plugins:
            plugin_keylist = self.plugins[plugin]['instance'].keylist_mapping()
            if not plugin_keylist:
                continue
            for k, v in plugin_keylist.items():
                o = keylist.get(k)
                if o:
                    if isinstance(o, set):
                        if isinstance(v, set):
                            o.update(v)
                        else:
                            o.update([v])
                    else:
                        o = set(o)
                        if isinstance(v, set):
                            o.update(v)
                        else:
                            o.update([v])
                else:
                    o = v
                keylist[k] = o
        return keylist


class LogProcessorDaemon(Daemon):
    def __init__(self, conf):
        c = conf.get('log-processor')
        super(LogProcessorDaemon, self).__init__(c)
        self.total_conf = conf
        self.logger = get_logger(c)
        self.log_processor = LogProcessor(conf, self.logger)
        self.lookback_hours = int(c.get('lookback_hours', '120'))
        self.lookback_window = int(c.get('lookback_window',
                                   str(self.lookback_hours)))
        self.log_processor_account = c['swift_account']
        self.log_processor_container = c.get('container_name',
                                             'log_processing_data')

    def run_once(self):
        self.logger.info("Beginning log processing")
        start = time.time()
        if self.lookback_hours == 0:
            lookback_start = None
            lookback_end = None
        else:
            lookback_start = datetime.datetime.now() - \
                             datetime.timedelta(hours=self.lookback_hours)
            lookback_start = lookback_start.strftime('%Y%m%d')
            if self.lookback_window == 0:
                lookback_end = None
            else:
                lookback_end = datetime.datetime.now() - \
                               datetime.timedelta(hours=self.lookback_hours) + \
                               datetime.timedelta(hours=self.lookback_window)
                lookback_end = lookback_end.strftime('%Y%m%d')
        self.logger.debug('lookback_start: %s' % lookback_start)
        self.logger.debug('lookback_end: %s' % lookback_end)
        try:
            processed_files_stream = self.log_processor.get_object_data(
                                        self.log_processor_account,
                                        self.log_processor_container,
                                        'processed_files.pickle.gz',
                                        compressed=True)
            buf = ''.join(x for x in processed_files_stream)
            if buf:
                already_processed_files = cPickle.loads(buf)
            else:
                already_processed_files = set()
        except:
            already_processed_files = set()
        self.logger.debug('found %d processed files' % len(already_processed_files))
        logs_to_process = self.log_processor.get_data_list(lookback_start,
                                                           lookback_end,
                                                           already_processed_files)
        self.logger.info('loaded %d files to process' % len(logs_to_process))
        if not logs_to_process:
            self.logger.info("Log processing done (%0.2f minutes)" %
                        ((time.time()-start)/60))
            return

        # map
        processor_args = (self.total_conf, self.logger)
        results = multiprocess_collate(processor_args, logs_to_process)

        #reduce
        aggr_data = {}
        processed_files = already_processed_files
        for item, data in results:
            # since item contains the plugin and the log name, new plugins will
            # "reprocess" the file and the results will be in the final csv.
            processed_files.append(item)
            for k, d in data.items():
                existing_data = aggr_data.get(k, {})
                for i, j in d.items():
                    current = existing_data.get(i, 0)
                    # merging strategy for key collisions is addition
                    # processing plugins need to realize this
                    existing_data[i] = current + j
                aggr_data[k] = existing_data

        # group
        # reduce a large number of keys in aggr_data[k] to a small number of
        # output keys
        keylist_mapping = self.log_processor.generate_keylist_mapping()
        final_info = collections.defaultdict(dict)
        for account, data in aggr_data.items():
            for key, mapping in keylist_mapping.items():
                if isinstance(mapping, (list, set)):
                    value = 0
                    for k in mapping:
                        try:
                            value += data[k]
                        except KeyError:
                            pass
                else:
                    try:
                        value = data[mapping]
                    except KeyError:
                        value = 0
                final_info[account][key] = value

        # output
        sorted_keylist_mapping = sorted(keylist_mapping)
        columns = 'bill_ts,data_ts,account,' + ','.join(sorted_keylist_mapping)
        print columns
        for (account, year, month, day, hour), d in final_info.items():
            bill_ts = ''
            data_ts = '%s/%s/%s %s:00:00' % (year, month, day, hour)
            row = [bill_ts, data_ts]
            row.append('%s' % account)
            for k in sorted_keylist_mapping:
                row.append('%s'%d[k])
            print ','.join(row)

        # cleanup
        s = cPickle.dumps(processed_files, cPickle.HIGHEST_PROTOCOL)
        f = cStringIO.StringIO(s)
        self.log_processor.internal_proxy.upload_file(f,
                                        self.log_processor_account,
                                        self.log_processor_container,
                                        'processed_files.pickle.gz')

        self.logger.info("Log processing done (%0.2f minutes)" %
                        ((time.time()-start)/60))

def multiprocess_collate(processor_args, logs_to_process):
    '''yield hourly data from logs_to_process'''
    worker_count = multiprocessing.cpu_count()
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
    for x in logs_to_process:
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
            if count >= len(logs_to_process):
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
            ret = p.process_one_file(*item)
            out_queue.put((item, ret))