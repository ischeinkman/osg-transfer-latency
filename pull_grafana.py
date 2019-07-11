#%%
import sys
import requests

#%%
_MILLIS = 1.0
_SECONDS = 1000.0 * _MILLIS
_MINUTES = 60.0 * _SECONDS
_HOURS = 60.0 * _MINUTES

_DEFAULT_PRE_RUN_OFFSET = 5.0 * _MINUTES
_DEFAULT_POST_RUN_OFFSET = 5.0 * _MINUTES


#%%
URL_BASE = 'http://condorflux.t2.ucsd.edu:8086/query'

SQL_FMT = ('SELECT max("value") FROM JobsAccumPostExecuteTime ' + 
    'WHERE host =~ /^sdsc-76.t2.ucsd.edu$/ ' + 
        'AND time > %ds ' + 
        'AND time < %ds ' + 
        'GROUP BY time(1s) fill(none)'
)

def build_query_url(start_epoch_seconds, end_epoch_seconds):
    sql = SQL_FMT%(start_epoch_seconds, end_epoch_seconds)
    rawargs = ('db=htcondor&q=' + sql + '&epoch=ms').replace(' ', '%20').replace('"', "%22")
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
def get_data(start_epoch_seconds, end_epoch_seconds):
    url = build_query_url(start_epoch_seconds, end_epoch_seconds)
    outpt = requests.get(url, verify=False )
    parsed = parse_output(outpt.content)
    filt = filter_extras(parsed)
    return filt 



#%%
if __name__ == "__main__":
    import time
    print(get_data(int(time.time()) - 24 * _HOURS/_SECONDS, int(time.time())))