# Copyright (c) 2010-2011 OpenStack, LLC.
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
import hashlib

from swift.common.internal_proxy import InternalProxy
from swift.common.exceptions import ChunkReadTimeout
from swift.common.utils import get_logger, readconf
from swift.common.daemon import Daemon


class BadFileDownload(Exception):
    pass


class LogProcessor(object):
    """Load plugins, process logs"""

    def __init__(self, conf, logger):
        if isinstance(logger, tuple):
            self.logger = get_logger(*logger)
        else:
            self.logger = logger

        self.conf = conf
        self._internal_proxy = None

        # load the processing plugins
        self.plugins = {}
        plugin_prefix = 'log-processor-'
        for section in (x for x in conf if x.startswith(plugin_prefix)):
            plugin_name = section[len(plugin_prefix):]
            plugin_conf = conf.get(section, {})
            self.plugins[plugin_name] = plugin_conf
            class_path = self.plugins[plugin_name]['class_path']
            import_target, class_name = class_path.rsplit('.', 1)
            module = __import__(import_target, fromlist=[import_target])
            klass = getattr(module, class_name)
            self.plugins[plugin_name]['instance'] = klass(plugin_conf)
            self.logger.debug(_('Loaded plugin "%s"') % plugin_name)

    @property
    def internal_proxy(self):
        if self._internal_proxy is None:
            stats_conf = self.conf.get('log-processor', {})
            proxy_server_conf_loc = stats_conf.get('proxy_server_conf',
                                            '/etc/swift/proxy-server.conf')
            proxy_server_conf = appconfig(
                                        'config:%s' % proxy_server_conf_loc,
                                        name='proxy-server')
            self._internal_proxy = InternalProxy(proxy_server_conf,
                                                 self.logger,
                                                 retries=3)
        return self._internal_proxy

    def process_one_file(self, plugin_name, account, container, object_name):
        self.logger.info(_('Processing %(obj)s with plugin "%(plugin)s"') %
                    {'obj': '/'.join((account, container, object_name)),
                     'plugin': plugin_name})
        # get an iter of the object data
        compressed = object_name.endswith('.gz')
        stream = self.get_object_data(account, container, object_name,
                                      compressed=compressed)
        # look up the correct plugin and send the stream to it
        return self.plugins[plugin_name]['instance'].process(stream,
                                                             account,
                                                             container,
                                                             object_name)

    def get_data_list(self, start_date=None, end_date=None,
                      listing_filter=None):
        total_list = []
        for plugin_name, data in self.plugins.items():
            account = data['swift_account']
            container = data['container_name']
            listing = self.get_container_listing(account,
                                                 container,
                                                 start_date,
                                                 end_date)
            for object_name in listing:
                # The items in this list end up being passed as positional
                # parameters to process_one_file.
                x = (plugin_name, account, container, object_name)
                if x not in listing_filter:
                    total_list.append(x)
        return total_list

    def get_container_listing(self, swift_account, container_name,
                              start_date=None, end_date=None,
                              listing_filter=None):
        '''
        Get a container listing, filtered by start_date, end_date, and
        listing_filter. Dates, if given, must be in YYYYMMDDHH format
        '''
        search_key = None
        if start_date is not None:
            try:
                parsed_date = time.strptime(start_date, '%Y%m%d%H')
            except ValueError:
                pass
            else:
                year = '%04d' % parsed_date.tm_year
                month = '%02d' % parsed_date.tm_mon
                day = '%02d' % parsed_date.tm_mday
                hour = '%02d' % parsed_date.tm_hour
                search_key = '/'.join([year, month, day, hour])
        end_key = None
        if end_date is not None:
            try:
                parsed_date = time.strptime(end_date, '%Y%m%d%H')
            except ValueError:
                pass
            else:
                year = '%04d' % parsed_date.tm_year
                month = '%02d' % parsed_date.tm_mon
                day = '%02d' % parsed_date.tm_mday
                # Since the end_marker filters by <, we need to add something
                # to make sure we get all the data under the last hour. Adding
                # one to the hour should be all-inclusive.
                hour = '%02d' % (parsed_date.tm_hour + 1)
                end_key = '/'.join([year, month, day, hour])
        container_listing = self.internal_proxy.get_container_list(
                                    swift_account,
                                    container_name,
                                    marker=search_key,
                                    end_marker=end_key)
        results = []
        if container_listing is not None:
            if listing_filter is None:
                listing_filter = set()
            for item in container_listing:
                name = item['name']
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
        # Python decompressing gzip chunk-by-chunk
        # http://stackoverflow.com/questions/2423866
        d = zlib.decompressobj(16 + zlib.MAX_WBITS)
        try:
            for chunk in o:
                if compressed:
                    try:
                        chunk = d.decompress(chunk)
                    except zlib.error:
                        self.logger.debug(_('Bad compressed data for %s')
                            % '/'.join((swift_account, container_name,
                                        object_name)))
                        raise BadFileDownload()  # bad compressed data
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
    """
    Gather raw log data and farm proccessing to generate a csv that is
    uploaded to swift.
    """

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
        self.worker_count = int(c.get('worker_count', '1'))

    def run_once(self):
        self.logger.info(_("Beginning log processing"))
        start = time.time()
        if self.lookback_hours == 0:
            lookback_start = None
            lookback_end = None
        else:
            delta_hours = datetime.timedelta(hours=self.lookback_hours)
            lookback_start = datetime.datetime.now() - delta_hours
            lookback_start = lookback_start.strftime('%Y%m%d%H')
            if self.lookback_window == 0:
                lookback_end = None
            else:
                delta_window = datetime.timedelta(hours=self.lookback_window)
                lookback_end = datetime.datetime.now() - \
                               delta_hours + \
                               delta_window
                lookback_end = lookback_end.strftime('%Y%m%d%H')
        self.logger.debug('lookback_start: %s' % lookback_start)
        self.logger.debug('lookback_end: %s' % lookback_end)
        try:
            # Note: this file (or data set) will grow without bound.
            # In practice, if it becomes a problem (say, after many months of
            # running), one could manually prune the file to remove older
            # entries. Automatically pruning on each run could be dangerous.
            # There is not a good way to determine when an old entry should be
            # pruned (lookback_hours could be set to anything and could change)
            processed_files_stream = self.log_processor.get_object_data(
                                        self.log_processor_account,
                                        self.log_processor_container,
                                        'processed_files.pickle.gz',
                                        compressed=True)
            buf = '\n'.join(x for x in processed_files_stream)
            if buf:
                already_processed_files = cPickle.loads(buf)
            else:
                already_processed_files = set()
        except Exception:
            already_processed_files = set()
        self.logger.debug(_('found %d processed files') % \
                          len(already_processed_files))
        logs_to_process = self.log_processor.get_data_list(lookback_start,
                                                       lookback_end,
                                                       already_processed_files)
        self.logger.info(_('loaded %d files to process') %
                         len(logs_to_process))
        if not logs_to_process:
            self.logger.info(_("Log processing done (%0.2f minutes)") %
                        ((time.time() - start) / 60))
            return

        # map
        processor_args = (self.total_conf, self.logger)
        results = multiprocess_collate(processor_args, logs_to_process,
                                       self.worker_count)

        #reduce
        aggr_data = {}
        processed_files = already_processed_files
        for item, data in results:
            # since item contains the plugin and the log name, new plugins will
            # "reprocess" the file and the results will be in the final csv.
            processed_files.add(item)
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
        columns = 'data_ts,account,' + ','.join(sorted_keylist_mapping)
        out_buf = [columns]
        for (account, year, month, day, hour), d in final_info.items():
            data_ts = '%s/%s/%s %s:00:00' % (year, month, day, hour)
            row = [data_ts]
            row.append('%s' % account)
            for k in sorted_keylist_mapping:
                row.append('%s' % d[k])
            out_buf.append(','.join(row))
        out_buf = '\n'.join(out_buf)
        h = hashlib.md5(out_buf).hexdigest()
        upload_name = time.strftime('%Y/%m/%d/%H/') + '%s.csv.gz' % h
        f = cStringIO.StringIO(out_buf)
        self.log_processor.internal_proxy.upload_file(f,
                                        self.log_processor_account,
                                        self.log_processor_container,
                                        upload_name)

        # cleanup
        s = cPickle.dumps(processed_files, cPickle.HIGHEST_PROTOCOL)
        f = cStringIO.StringIO(s)
        self.log_processor.internal_proxy.upload_file(f,
                                        self.log_processor_account,
                                        self.log_processor_container,
                                        'processed_files.pickle.gz')

        self.logger.info(_("Log processing done (%0.2f minutes)") %
                        ((time.time() - start) / 60))


def multiprocess_collate(processor_args, logs_to_process, worker_count):
    '''yield hourly data from logs_to_process'''
    results = []
    in_queue = multiprocessing.Queue()
    out_queue = multiprocessing.Queue()
    for _junk in range(worker_count):
        p = multiprocessing.Process(target=collate_worker,
                                    args=(processor_args,
                                          in_queue,
                                          out_queue))
        p.start()
        results.append(p)
    for x in logs_to_process:
        in_queue.put(x)
    for _junk in range(worker_count):
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
