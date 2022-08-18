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

import html
import os
import random
import re
import string
import tempfile

from swift.common.middleware.x_profile.exceptions import PLOTLIBNotInstalled
from swift.common.middleware.x_profile.exceptions import ODFLIBNotInstalled
from swift.common.middleware.x_profile.exceptions import NotFoundException
from swift.common.middleware.x_profile.exceptions import MethodNotAllowed
from swift.common.middleware.x_profile.exceptions import DataLoadFailure
from swift.common.middleware.x_profile.exceptions import ProfileException
from swift.common.middleware.x_profile.profile_model import Stats2

PLOTLIB_INSTALLED = True
try:
    import matplotlib
    # use agg backend for writing to file, not for rendering in a window.
    # otherwise some platform will complain "no display name and $DISPLAY
    # environment variable"
    matplotlib.use('agg')
    import matplotlib.pyplot as plt
except ImportError:
    PLOTLIB_INSTALLED = False


empty_description = """
        The default profile of current process or the profile you requested is
        empty. <input type="submit" name="refresh" value="Refresh"/>
"""

profile_tmpl = """
              <select name="profile">
                <option value="current">current</option>
                <option value="all">all</option>
                ${profile_list}
              </select>
"""

sort_tmpl = """
              <select name="sort">
                <option value="time">time</option>
                <option value="cumulative">cumulative</option>
                <option value="calls">calls</option>
                <option value="pcalls">pcalls</option>
                <option value="name">name</option>
                <option value="file">file</option>
                <option value="module">module</option>
                <option value="line">line</option>
                <option value="nfl">nfl</option>
                <option value="stdname">stdname</option>
              </select>
"""

limit_tmpl = """
              <select name="limit">
                <option value="-1">all</option>
                <option value="0.1">10%</option>
                <option value="0.2">20%</option>
                <option value="0.3">30%</option>
                <option value="10">10</option>
                <option value="20">20</option>
                <option value="30">30</option>
                <option value="50">50</option>
                <option value="100">100</option>
                <option value="200">200</option>
                <option value="300">300</option>
                <option value="400">400</option>
                <option value="500">500</option>
              </select>
"""

fulldirs_tmpl = """
              <input type="checkbox" name="fulldirs" value="1"
              ${fulldir_checked}/>
"""

mode_tmpl = """
              <select name="mode">
                <option value="stats">stats</option>
                <option value="callees">callees</option>
                <option value="callers">callers</option>
              </select>
"""

nfl_filter_tmpl = """
              <input type="text" name="nfl_filter" value="${nfl_filter}"
              placeholder="filename part" />
"""

formelements_tmpl = """
      <div>
        <table>
          <tr>
            <td>
              <strong>Profile</strong>
            <td>
              <strong>Sort</strong>
            </td>
            <td>
              <strong>Limit</strong>
            </td>
            <td>
              <strong>Full Path</strong>
            </td>
            <td>
              <strong>Filter</strong>
            </td>
            <td>
            </td>
            <td>
              <strong>Plot Metric</strong>
            </td>
            <td>
              <strong>Plot Type</strong>
            <td>
            </td>
            <td>
              <strong>Format</strong>
            </td>
            <td>
            <td>
            </td>
            <td>
            </td>

          </tr>
          <tr>
            <td>
               ${profile}
            <td>
               ${sort}
            </td>
            <td>
               ${limit}
            </td>
            <td>
              ${fulldirs}
            </td>
            <td>
              ${nfl_filter}
            </td>
            <td>
              <input type="submit" name="query" value="query"/>
            </td>
            <td>
              <select name='metric'>
                <option value='nc'>call count</option>
                <option value='cc'>primitive call count</option>
                <option value='tt'>total time</option>
                <option value='ct'>cumulative time</option>
              </select>
            </td>
            <td>
              <select name='plottype'>
                <option value='bar'>bar</option>
                <option value='pie'>pie</option>
              </select>
            <td>
              <input type="submit" name="plot" value="plot"/>
            </td>
            <td>
              <select name='format'>
                <option value='default'>binary</option>
                <option value='json'>json</option>
                <option value='csv'>csv</option>
                <option value='ods'>ODF.ods</option>
              </select>
            </td>
            <td>
              <input type="submit" name="download" value="download"/>
            </td>
            <td>
              <input type="submit" name="clear" value="clear"/>
            </td>
          </tr>
        </table>
      </div>
"""

index_tmpl = """
<html>
  <head>
    <title>profile results</title>
    <style>
    <!--
      tr.normal { background-color: #ffffff }
      tr.hover { background-color: #88eeee }
    //-->
    </style>
  </head>
  <body>

    <form action="${action}" method="POST">

      <div class="form-text">
        ${description}
      </div>
      <hr />
      ${formelements}

    </form>
    <pre>
${profilehtml}
    </pre>

  </body>
</html>
"""


class HTMLViewer(object):

    format_dict = {'default': 'application/octet-stream',
                   'json': 'application/json',
                   'csv': 'text/csv',
                   'ods': 'application/vnd.oasis.opendocument.spreadsheet',
                   'python': 'text/html'}

    def __init__(self, app_path, profile_module, profile_log):
        self.app_path = app_path
        self.profile_module = profile_module
        self.profile_log = profile_log

    def _get_param(self, query_dict, key, default=None, multiple=False):
        value = query_dict.get(key, default)
        if value is None or value == '':
            return default
        if multiple:
            return value
        if isinstance(value, list):
            return int(value[0]) if isinstance(default, int) else value[0]
        else:
            return value

    def render(self, url, method, path_entry, query_dict, clear_callback):
        plot = self._get_param(query_dict, 'plot', None)
        download = self._get_param(query_dict, 'download', None)
        clear = self._get_param(query_dict, 'clear', None)
        action = plot or download or clear
        profile_id = self._get_param(query_dict, 'profile', 'current')
        sort = self._get_param(query_dict, 'sort', 'time')
        limit = self._get_param(query_dict, 'limit', -1)
        fulldirs = self._get_param(query_dict, 'fulldirs', 0)
        nfl_filter = self._get_param(query_dict, 'nfl_filter', '').strip()
        metric_selected = self._get_param(query_dict, 'metric', 'cc')
        plot_type = self._get_param(query_dict, 'plottype', 'bar')
        download_format = self._get_param(query_dict, 'format', 'default')
        content = ''
        # GET  /__profile, POST /__profile
        if len(path_entry) == 2 and method in ['GET', 'POST']:
            log_files = self.profile_log.get_logfiles(profile_id)
            if action == 'plot':
                content, headers = self.plot(log_files, sort, limit,
                                             nfl_filter, metric_selected,
                                             plot_type)
            elif action == 'download':
                content, headers = self.download(log_files, sort, limit,
                                                 nfl_filter, download_format)
            else:
                if action == 'clear':
                    self.profile_log.clear(profile_id)
                    clear_callback and clear_callback()
                content, headers = self.index_page(log_files, sort, limit,
                                                   fulldirs, nfl_filter,
                                                   profile_id, url)
        # GET /__profile__/all
        # GET /__profile__/current
        # GET /__profile__/profile_id
        # GET /__profile__/profile_id/
        # GET /__profile__/profile_id/account.py:50(GETorHEAD)
        # GET /__profile__/profile_id/swift/proxy/controllers
        #      /account.py:50(GETorHEAD)
        # with QUERY_STRING:   ?format=[default|json|csv|ods]
        elif len(path_entry) > 2 and method == 'GET':
            profile_id = path_entry[2]
            log_files = self.profile_log.get_logfiles(profile_id)
            pids = self.profile_log.get_all_pids()
            # return all profiles in a json format by default.
            # GET /__profile__/
            if profile_id == '':
                content = '{"profile_ids": ["' + '","'.join(pids) + '"]}'
                headers = [('content-type', self.format_dict['json'])]
            else:
                if len(path_entry) > 3 and path_entry[3] != '':
                    nfl_filter = '/'.join(path_entry[3:])
                    if path_entry[-1].find(':0') == -1:
                        nfl_filter = '/' + nfl_filter
                content, headers = self.download(log_files, sort, -1,
                                                 nfl_filter, download_format)
            headers.append(('Access-Control-Allow-Origin', '*'))
        else:
            raise MethodNotAllowed('method %s is not allowed.' % method)
        return content, headers

    def index_page(self, log_files=None, sort='time', limit=-1,
                   fulldirs=0, nfl_filter='', profile_id='current', url='#'):
        headers = [('content-type', 'text/html')]
        if len(log_files) == 0:
            return empty_description, headers
        try:
            stats = Stats2(*log_files)
        except (IOError, ValueError):
            raise DataLoadFailure('Can not load profile data from %s.'
                                  % log_files)
        if not fulldirs:
            stats.strip_dirs()
        stats.sort_stats(sort)
        nfl_filter_esc = nfl_filter.replace(r'(', r'\(').replace(r')', r'\)')
        amount = [nfl_filter_esc, limit] if nfl_filter_esc else [limit]
        profile_html = self.generate_stats_html(stats, self.app_path,
                                                profile_id, *amount)
        description = "Profiling information is generated by using\
                      '%s' profiler." % self.profile_module
        sort_repl = '<option value="%s">' % sort
        sort_selected = '<option value="%s" selected>' % sort
        sort = sort_tmpl.replace(sort_repl, sort_selected)
        plist = ''.join(['<option value="%s">%s</option>' % (p, p)
                         for p in self.profile_log.get_all_pids()])
        profile_element = string.Template(profile_tmpl).substitute(
            {'profile_list': plist})
        profile_repl = '<option value="%s">' % profile_id
        profile_selected = '<option value="%s" selected>' % profile_id
        profile_element = profile_element.replace(profile_repl,
                                                  profile_selected)
        limit_repl = '<option value="%s">' % limit
        limit_selected = '<option value="%s" selected>' % limit
        limit = limit_tmpl.replace(limit_repl, limit_selected)
        fulldirs_checked = 'checked' if fulldirs else ''
        fulldirs_element = string.Template(fulldirs_tmpl).substitute(
            {'fulldir_checked': fulldirs_checked})
        nfl_filter_element = string.Template(nfl_filter_tmpl).\
            substitute({'nfl_filter': nfl_filter})
        form_elements = string.Template(formelements_tmpl).substitute(
            {'description': description,
             'action': url,
             'profile': profile_element,
             'sort': sort,
             'limit': limit,
             'fulldirs': fulldirs_element,
             'nfl_filter': nfl_filter_element,
             }
        )
        content = string.Template(index_tmpl).substitute(
            {'formelements': form_elements,
             'action': url,
             'description': description,
             'profilehtml': profile_html,
             })
        return content, headers

    def download(self, log_files, sort='time', limit=-1, nfl_filter='',
                 output_format='default'):
        if len(log_files) == 0:
            raise NotFoundException('no log file found')
        try:
            nfl_esc = nfl_filter.replace(r'(', r'\(').replace(r')', r'\)')
            # remove the slash that is intentionally added in the URL
            # to avoid failure of filtering stats data.
            if nfl_esc.startswith('/'):
                nfl_esc = nfl_esc[1:]
            stats = Stats2(*log_files)
            stats.sort_stats(sort)
            if output_format == 'python':
                data = self.format_source_code(nfl_filter)
            elif output_format == 'json':
                data = stats.to_json(nfl_esc, limit)
            elif output_format == 'csv':
                data = stats.to_csv(nfl_esc, limit)
            elif output_format == 'ods':
                data = stats.to_ods(nfl_esc, limit)
            else:
                data = stats.print_stats()
            return data, [('content-type', self.format_dict[output_format])]
        except ODFLIBNotInstalled:
            raise
        except Exception as ex:
            raise ProfileException('Data download error: %s' % ex)

    def plot(self, log_files, sort='time', limit=10, nfl_filter='',
             metric_selected='cc', plot_type='bar'):
        if not PLOTLIB_INSTALLED:
            raise PLOTLIBNotInstalled('python-matplotlib not installed.')
        if len(log_files) == 0:
            raise NotFoundException('no log file found')
        try:
            stats = Stats2(*log_files)
            stats.sort_stats(sort)
            stats_dict = stats.stats
            __, func_list = stats.get_print_list([nfl_filter, limit])
            nfls = []
            performance = []
            names = {'nc': 'Total Call Count', 'cc': 'Primitive Call Count',
                     'tt': 'Total Time', 'ct': 'Cumulative Time'}
            for func in func_list:
                cc, nc, tt, ct, __ = stats_dict[func]
                metric = {'cc': cc, 'nc': nc, 'tt': tt, 'ct': ct}
                nfls.append(func[2])
                performance.append(metric[metric_selected])
            y_pos = range(len(nfls))
            error = [random.random() for _unused in y_pos]
            plt.clf()
            if plot_type == 'pie':
                plt.pie(x=performance, explode=None, labels=nfls,
                        autopct='%1.1f%%')
            else:
                plt.barh(y_pos, performance, xerr=error, align='center',
                         alpha=0.4)
                plt.yticks(y_pos, nfls)
                plt.xlabel(names[metric_selected])
            plt.title('Profile Statistics (by %s)' % names[metric_selected])
            # plt.gcf().tight_layout(pad=1.2)
            with tempfile.TemporaryFile() as profile_img:
                plt.savefig(profile_img, format='png', dpi=300)
                profile_img.seek(0)
                data = profile_img.read()
                return data, [('content-type', 'image/jpg')]
        except Exception as ex:
            raise ProfileException('plotting results failed due to %s' % ex)

    def format_source_code(self, nfl):
        nfls = re.split('[:()]', nfl)
        file_path = nfls[0]
        try:
            lineno = int(nfls[1])
        except (TypeError, ValueError, IndexError):
            lineno = 0
        # for security reason, this need to be fixed.
        if not file_path.endswith('.py'):
            return 'The file type are forbidden to access!'
        try:
            data = []
            i = 0
            with open(file_path) as f:
                lines = f.readlines()
                max_width = str(len(str(len(lines))))
                fmt = '<span id="L%d" rel="#L%d">%' + max_width\
                    + 'd|<code>%s</code></span>'
                for line in lines:
                    el = html.escape(line)
                    i = i + 1
                    if i == lineno:
                        fmt2 = '<span id="L%d" style="background-color: \
                            rgb(127,255,127)">%' + max_width +\
                            'd|<code>%s</code></span>'
                        data.append(fmt2 % (i, i, el))
                    else:
                        data.append(fmt % (i, i, i, el))
            data = ''.join(data)
        except Exception:
            return 'Can not access the file %s.' % file_path
        return '<pre>%s</pre>' % data

    def generate_stats_html(self, stats, app_path, profile_id, *selection):
        html = []
        for filename in stats.files:
            html.append('<p>%s</p>' % filename)
        try:
            for func in stats.top_level:
                html.append('<p>%s</p>' % func[2])
            html.append('%s function calls' % stats.total_calls)
            if stats.total_calls != stats.prim_calls:
                html.append("(%d primitive calls)" % stats.prim_calls)
            html.append('in %.3f seconds' % stats.total_tt)
            if stats.fcn_list:
                stat_list = stats.fcn_list[:]
                msg = "<p>Ordered by: %s</p>" % stats.sort_type
            else:
                stat_list = stats.stats.keys()
                msg = '<p>Random listing order was used</p>'
            for sel in selection:
                stat_list, msg = stats.eval_print_amount(sel, stat_list, msg)
            html.append(msg)
            html.append('<table style="border-width: 1px">')
            if stat_list:
                html.append('<tr><th>#</th><th>Call Count</th>\
                                    <th>Total Time</th><th>Time/Call</th>\
                                    <th>Cumulative Time</th>\
                                    <th>Cumulative Time/Call</th>\
                                    <th>Filename:Lineno(Function)</th>\
                                    <th>JSON</th>\
                                </tr>')
                count = 0
                for func in stat_list:
                    count = count + 1
                    html.append('<tr onMouseOver="this.className=\'hover\'"\
                                     onMouseOut="this.className=\'normal\'">\
                                     <td>%d)</td>' % count)
                    cc, nc, tt, ct, __ = stats.stats[func]
                    c = str(nc)
                    if nc != cc:
                        c = c + '/' + str(cc)
                    html.append('<td>%s</td>' % c)
                    html.append('<td>%f</td>' % tt)
                    if nc == 0:
                        html.append('<td>-</td>')
                    else:
                        html.append('<td>%f</td>' % (float(tt) / nc))
                    html.append('<td>%f</td>' % ct)
                    if cc == 0:
                        html.append('<td>-</td>')
                    else:
                        html.append('<td>%f</td>' % (float(ct) / cc))
                    nfls = html.escape(stats.func_std_string(func))
                    if nfls.split(':')[0] not in ['', 'profile'] and\
                            os.path.isfile(nfls.split(':')[0]):
                        html.append('<td><a href="%s/%s%s?format=python#L%d">\
                                     %s</a></td>' % (app_path, profile_id,
                                                     nfls, func[1], nfls))
                    else:
                        html.append('<td>%s</td>' % nfls)
                    if not nfls.startswith('/'):
                        nfls = '/' + nfls
                    html.append('<td><a href="%s/%s%s?format=json">\
                                --></a></td></tr>' % (app_path,
                                                      profile_id, nfls))
        except Exception as ex:
            html.append("Exception:" + str(ex))
        return ''.join(html)
