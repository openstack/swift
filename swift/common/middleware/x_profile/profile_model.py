# Copyright (c) 2010-2012 OpenStack, LLC.
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

import glob
import json
import os
import pstats
import tempfile
import time

from swift import gettext_ as _
from swift.common.middleware.x_profile.exceptions import ODFLIBNotInstalled


ODFLIB_INSTALLED = True
try:
    from odf.opendocument import OpenDocumentSpreadsheet
    from odf.table import Table, TableRow, TableCell
    from odf.text import P
except ImportError:
    ODFLIB_INSTALLED = False


class Stats2(pstats.Stats):

    def __init__(self, *args, **kwds):
        pstats.Stats.__init__(self, *args, **kwds)

    def func_to_dict(self, func):
        return {'module': func[0], 'line': func[1], 'function': func[2]}

    def func_std_string(self, func):
        return pstats.func_std_string(func)

    def to_json(self, *selection):
        d = dict()
        d['files'] = [f for f in self.files]
        d['prim_calls'] = (self.prim_calls)
        d['total_calls'] = (self.total_calls)
        if hasattr(self, 'sort_type'):
            d['sort_type'] = self.sort_type
        else:
            d['sort_type'] = 'random'
        d['total_tt'] = (self.total_tt)
        if self.fcn_list:
            stat_list = self.fcn_list[:]
        else:
            stat_list = self.stats.keys()
        for s in selection:
            stat_list, __ = self.eval_print_amount(s, stat_list, '')

        self.calc_callees()
        function_calls = []
        for func in stat_list:
            cc, nc, tt, ct, callers = self.stats[func]
            fdict = dict()
            fdict.update(self.func_to_dict(func))
            fdict.update({'cc': (cc), 'nc': (nc), 'tt': (tt),
                          'ct': (ct)})
            if self.all_callees:
                fdict.update({'callees': []})
                for key in self.all_callees[func]:
                    cee = self.func_to_dict(key)
                    metric = self.all_callees[func][key]
                    # FIXME: eventlet profiler don't provide full list of
                    # the metrics
                    if type(metric) is tuple:
                        cc1, nc1, tt1, ct1 = metric
                        cee.update({'cc': cc1, 'nc': nc1, 'tt': tt1,
                                    'ct': ct1})
                    else:
                        cee['nc'] = metric
                    fdict['callees'].append(cee)
            cer = []
            for caller in callers:
                fd = self.func_to_dict(caller)
                metric2 = callers[caller]
                if isinstance(metric2, tuple):
                    cc2, nc2, tt2, ct2 = metric2
                    fd.update({'cc': cc2, 'nc': nc2, 'tt': tt2, 'ct': ct2})
                else:
                    fd.update({'nc': metric2})
                cer.append(fd)
            fdict.update({'callers': cer})
            function_calls.append(fdict)
        d['stats'] = function_calls
        return json.dumps(d, indent=2)

    def to_csv(self, *selection):
        if self.fcn_list:
            stat_list = self.fcn_list[:]
            order_text = "Ordered by: " + self.sort_type + '\r\n'
        else:
            stat_list = self.stats.keys()
            order_text = "Random listing order was used\r\n"
        for s in selection:
            stat_list, __ = self.eval_print_amount(s, stat_list, '')

        csv = '%d function calls (%d primitive calls) in %.6f seconds.' % (
            self.total_calls, self.prim_calls, self.total_tt)
        csv = csv + order_text + 'call count(nc), primitive call count(cc), \
                                  total time(tt), time per call, \
                                  cumulative time(ct), time per call, \
                                  function\r\n'
        for func in stat_list:
            cc, nc, tt, ct, __ = self.stats[func]
            tpc = '' if nc == 0 else '%3f' % (tt / nc)
            cpc = '' if cc == 0 else '%3f' % (ct / cc)
            fn = '%s:%d(%s)' % (func[0], func[1], func[2])
            csv = csv + '%d,%d,%3f,%s,%3f,%s,%s\r\n' % (
                nc, cc, tt, tpc, ct, cpc, fn)
        return csv

    def to_ods(self, *selection):
        if not ODFLIB_INSTALLED:
            raise ODFLIBNotInstalled(_('odfpy not installed.'))
        if self.fcn_list:
            stat_list = self.fcn_list[:]
            order_text = "   Ordered by: " + self.sort_type + '\n'
        else:
            stat_list = self.stats.keys()
            order_text = "   Random listing order was used\n"
        for s in selection:
            stat_list, __ = self.eval_print_amount(s, stat_list, '')
        spreadsheet = OpenDocumentSpreadsheet()
        table = Table(name="Profile")
        for fn in self.files:
            tcf = TableCell()
            tcf.addElement(P(text=fn))
            trf = TableRow()
            trf.addElement(tcf)
            table.addElement(trf)

        tc_summary = TableCell()
        summary_text = '%d function calls (%d primitive calls) in %.6f \
                        seconds' % (self.total_calls, self.prim_calls,
                                    self.total_tt)
        tc_summary.addElement(P(text=summary_text))
        tr_summary = TableRow()
        tr_summary.addElement(tc_summary)
        table.addElement(tr_summary)

        tc_order = TableCell()
        tc_order.addElement(P(text=order_text))
        tr_order = TableRow()
        tr_order.addElement(tc_order)
        table.addElement(tr_order)

        tr_header = TableRow()
        tc_cc = TableCell()
        tc_cc.addElement(P(text='Total Call Count'))
        tr_header.addElement(tc_cc)

        tc_pc = TableCell()
        tc_pc.addElement(P(text='Primitive Call Count'))
        tr_header.addElement(tc_pc)

        tc_tt = TableCell()
        tc_tt.addElement(P(text='Total Time(seconds)'))
        tr_header.addElement(tc_tt)

        tc_pc = TableCell()
        tc_pc.addElement(P(text='Time Per call(seconds)'))
        tr_header.addElement(tc_pc)

        tc_ct = TableCell()
        tc_ct.addElement(P(text='Cumulative Time(seconds)'))
        tr_header.addElement(tc_ct)

        tc_pt = TableCell()
        tc_pt.addElement(P(text='Cumulative Time per call(seconds)'))
        tr_header.addElement(tc_pt)

        tc_nfl = TableCell()
        tc_nfl.addElement(P(text='filename:lineno(function)'))
        tr_header.addElement(tc_nfl)

        table.addElement(tr_header)

        for func in stat_list:
            cc, nc, tt, ct, __ = self.stats[func]
            tr_header = TableRow()
            tc_nc = TableCell()
            tc_nc.addElement(P(text=nc))
            tr_header.addElement(tc_nc)

            tc_pc = TableCell()
            tc_pc.addElement(P(text=cc))
            tr_header.addElement(tc_pc)

            tc_tt = TableCell()
            tc_tt.addElement(P(text=tt))
            tr_header.addElement(tc_tt)

            tc_tpc = TableCell()
            tc_tpc.addElement(P(text=(None if nc == 0 else float(tt) / nc)))
            tr_header.addElement(tc_tpc)

            tc_ct = TableCell()
            tc_ct.addElement(P(text=ct))
            tr_header.addElement(tc_ct)

            tc_tpt = TableCell()
            tc_tpt.addElement(P(text=(None if cc == 0 else float(ct) / cc)))
            tr_header.addElement(tc_tpt)

            tc_nfl = TableCell()
            tc_nfl.addElement(P(text=func))
            tr_header.addElement(tc_nfl)
            table.addElement(tr_header)

        spreadsheet.spreadsheet.addElement(table)
        with tempfile.TemporaryFile() as tmp_ods:
            spreadsheet.write(tmp_ods)
            tmp_ods.seek(0)
            data = tmp_ods.read()
            return data


class ProfileLog(object):

    def __init__(self, log_filename_prefix, dump_timestamp):
        self.log_filename_prefix = log_filename_prefix
        self.dump_timestamp = dump_timestamp

    def get_all_pids(self):
        profile_ids = [l.replace(self.log_filename_prefix, '') for l
                       in glob.glob(self.log_filename_prefix + '*')
                       if not l.endswith('.tmp')]
        return sorted(profile_ids, reverse=True)

    def get_logfiles(self, id_or_name):
        # The first file with timestamp in the sorted log_files
        # (PREFIX)(PROCESS_ID)-(TIMESTAMP)
        if id_or_name in ['all']:
            if self.dump_timestamp:
                latest_dict = {}
                for pid in self.get_all_pids():
                    [process_id, __] = pid.split('-')
                    if process_id not in latest_dict.keys():
                        latest_dict[process_id] = self.log_filename_prefix +\
                            pid
                log_files = latest_dict.values()
            else:
                log_files = [l for l in glob.glob(self.log_filename_prefix
                             + '*') if not l.endswith('.tmp')]
        else:
            pid = str(os.getpid()) if id_or_name in [None, '', 'current']\
                else id_or_name
            log_files = [l for l in glob.glob(self.log_filename_prefix +
                         pid + '*') if not l.endswith('.tmp')]
            if len(log_files) > 0:
                log_files = sorted(log_files, reverse=True)[0:1]
        return log_files

    def dump_profile(self, profiler, pid):
        if self.log_filename_prefix:
            pfn = self.log_filename_prefix + str(pid)
            if self.dump_timestamp:
                pfn = pfn + "-" + str(time.time())
            tmpfn = pfn + ".tmp"
            profiler.dump_stats(tmpfn)
            os.rename(tmpfn, pfn)
            return pfn

    def clear(self, id_or_name):
        log_files = self.get_logfiles(id_or_name)
        for l in log_files:
            os.path.exists(l) and os.remove(l)
