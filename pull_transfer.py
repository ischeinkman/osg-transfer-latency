#%%
import sys
import urllib2
import time 
import os
#%%
_MILLIS = 0.001
_SECONDS = 1000.0 * _MILLIS
_MINUTES = 60.0 * _SECONDS
_HOURS = 60.0 * _MINUTES

_DEFAULT_PRE_RUN_OFFSET = 5.0 * _MINUTES
_DEFAULT_POST_RUN_OFFSET = 10.0 * _MINUTES

#%%
URL_BASE = 'https://graph.t2.ucsd.edu:3000/api/datasources/proxy/1/query'
SQL_FMT = ('SELECT non_negative_derivative(mean(bytes_recv),1s)*8 as "in" ' +
 'FROM "net" ' + 
 'WHERE "host" =~ /sdsc-76.t2.ucsd.edu/' + 
 'AND interface =~ /(vlan|eth|bond|vmbr|ens|enp|ib).*/ ' + 
 'AND interface !~ /veth/ ' +
 'AND time > %dms '+ 
 'AND time < %dms ' + 
 'GROUP BY time(10s), * fill(none)'
)
def build_query_url(start_epoch_seconds, end_epoch_seconds):
    sql = SQL_FMT%(start_epoch_seconds/_MILLIS, end_epoch_seconds/_MILLIS)
    rawargs = ('db=telegraf&q=' + sql + '&epoch=ms').replace(' ', '%20').replace('"', "%22")
    url = URL_BASE + '?' + rawargs 
    return url 

#%% 
import json
def parse_output(raw_content):
    parsed = json.loads(raw_content)
    return parsed['results'][0]['series'][0]['values']
    
def filter_extras(itms):
    retval = []
    assert(len(itms[0]) == 2)
    cur_val = itms[0][1]
    retval.append(itms[0])
    for itm in itms[1:]:
        if itm[1] != cur_val:
            retval.append(itm)
            cur_val = itm[1]
    return retval
def run_request(url):
    req = urllib2.Request(url, unverifiable=True)
    req.add_header('Authorization', os.environ.get('GAUTH') or '')
    return urllib2.urlopen(req, context=urllib2.ssl._create_unverified_context())

def get_data(start_epoch_seconds, end_epoch_seconds):
    url = build_query_url(start_epoch_seconds, end_epoch_seconds)
    outpt = run_request(url)
    parsed = parse_output(outpt.read())
    filt = filter_extras(parsed)
    return filt 


def run_as_postscript():
    wd = sys.argv[-1]
    logpath = wd.rstrip('/') + '/glideTester.log'
    logdata = ''
    with open(logpath, 'r') as fh:
        logdata = fh.read()
    relevants = [ln for ln in logdata.splitlines() if ln.endswith('submitted') or ln.endswith('Done')]
    TIME_FORMAT = '%a %b %d %H:%M:%S %Y'
    parsed = []
    for ln in relevants:
        splitidx = ln.index('2019') + 4
        timestamp_raw = ln[:splitidx]
        timestamp_obj = time.strptime(timestamp_raw, TIME_FORMAT)
        unixtime = time.mktime(timestamp_obj)
        kind = 'd' if ln.endswith('Done') else 's' if ln.endswith('submitted') else 'ERR'
        parsed.append((unixtime, kind))
    parsed.sort(key = lambda ent: ent[0])
    parsed.append([int(time.time()), 'd'])
    run_start = parsed[0][0]
    run_end = parsed[1][0]
    graf_data = get_data(run_start - _DEFAULT_PRE_RUN_OFFSET, run_end + _DEFAULT_POST_RUN_OFFSET)

    outfl = open(wd.rstrip('/') + '/pull_transfer.txt', 'w')
    outfl.write('\n'.join(map(str, graf_data)) + '\n')
    outfl.flush()


#%%
if __name__ == "__main__":
    run_as_postscript()
