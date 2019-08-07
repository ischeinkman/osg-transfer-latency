
import sys 
import os 

def parse_params(path):
    outpt = {}
    fh = open(path + '/parameters.cfg', 'r')
    fd = fh.read().split('\n')
    for ln in fd: 
        nocomment = ln.split('#', 1)[0].strip()
        if len(nocomment) == 0:
            continue 
        k, v = map(lambda p : p.strip(), nocomment.split('=', 1))
        if k == 'arguments':
            vp = v.split(' ')
            argmap = {
                'flags' : {},
                'noflag' : []
            }
            for itm in vp:
                if itm.startswith('--'):
                    if '=' in itm:
                        argk, argv = map(lambda p : p.strip(), itm.split('='))
                    else:
                        argk, argv = itm, True 
                    argmap['flags'][argk] = argv
                else: 
                    argmap['noflag'].append(itm) 
            outpt[k] = argmap
        else: 
            outpt[k] = v 
    return outpt 

def parse_grafana(path):
    outpt = {
        'raw' : []
    }
    fh = open(path + '/pull_grafana_out.txt', 'r')
    fd = fh.read().split('\n')
    for ln in fd: 
        if len(ln.strip()) == 0:
            continue
        ts, accum = ln.strip('[]\n ').split(', ')
        outpt['raw'].append((float(ts), float(accum)))
    outpt['raw'].sort(key = lambda itm: itm[0])
    if len(outpt['raw']) >= 2:
        outpt['time'] = outpt['raw'][-1][1] - outpt['raw'][0][1]
    else: 
        outpt['time'] = -1
    return outpt 

def parse_latency(path):
    latency_file = path.rstrip('/') + '/' + 'latency.txt'
    if os.path.exists(latency_file):
        fh = open(latency_file, 'r')
        fd = fh.read()
        lines = fd.splitlines()
        parsed = [(float(a.split(', ')[0]), float(a.split(', ')[1])) for a in lines]
    return {
        'rows' : parsed,
        'last' : parsed[-1][1]
    }

def parse_aborts(path):
    filename = path.rstrip('/') + '/' + 'con_400_run_0.log'
    fh = open(filename, 'r')
    fd = fh.read()
    import datagen
    new_dt = datagen.parse_conlog(fd)
    output = {
        'normal' : 0, 
        'abort' : 0, 
        'abort_list' : []
    }
    for evt in new_dt:
        if evt['MyType'] == 'JobAbortedEvent':
            output['abort'] += 1 
            output['abort_list'].append('400.0.' + str(int(evt['Proc'])))
        elif evt['MyType'] == 'JobTerminatedEvent':
            output['normal'] += 1 
    return output

def parse_fails(path):
    filename = path.rstrip('/') + '/' + 'jobfails.csv'
    fh = open(filename, 'r')
    fd = fh.read()
    fl = fd.splitlines()
    return [j for j in fl if len(j.strip()) > 0]

def parse_conlog_shadow_exceptions(path):
    filename = path.rstrip('/') + '/' + 'con_400_run_0.log'
    fh = open(filename, 'r')
    fd = fh.read()

    conlog_data = []
    lns = fd.splitlines()
    idx = 0 
    while idx < len(lns):
        ln = lns[idx]
        if not "Shadow exception!" in ln: 
            idx += 1
            continue 
        _, raw_run, raw_date, raw_time, _, _ = ln.split(' ')
        run = raw_run[1:-1]
        import time 
        cur_year = time.localtime().tm_year
        ts = time.mktime(time.strptime("{}/{} {}".format(cur_year, raw_date, raw_time), "%Y/%m/%d %H:%M:%S"))
        reason = lns[idx + 1].strip() if idx + 1 < len(lns) else "UNKNOWN"
        ent = {
            'run' : run,
            'timestamp' : ts, 
            'reason' : reason
        }
        conlog_data.append(ent)
        idx += 2
    return conlog_data

def parse_shadow_exceptions(path):
    conlog = path.rstrip('/') + '/' + 'con_400_run_0.log'
    conlog_fh = open(conlog, 'r')
    conlog_fd = conlog_fh.read().splitlines()
    valid_jobs = [ln.split(' ')[1][1:-5] for ln in conlog_fd if 'submitted' in ln]
    by_job = {}
    jobs_with_exceptions = set([])
    local_shadow = path.rstrip('/') + '/' + 'ShadowLog'
    shadowlog = local_shadow if os.path.isfile(local_shadow) else "/var/log/condor/ShadowLog"
    shadow_fh = open(shadowlog, 'r')
    shadow_fd = shadow_fh.read().splitlines()
    for ln in shadow_fd:
        jid = ""
        for cur_job in valid_jobs:
            shadow_form = "({}.{})".format(*[int(p) for p in cur_job.split('.')])
            if shadow_form in ln: 
                jid = cur_job
                break 
        if not jid in by_job:
            by_job[jid] = []
        by_job[jid].append(ln)
        if 'get_file(): ERROR: received' in ln:
            jobs_with_exceptions.add(jid)
    return {
        'jwe' : len(jobs_with_exceptions),
        'raw' : by_job,
    }
    



def parse_fail_unknowns(path):
    unknown_fails = []
    raw_aborts = parse_aborts(path)
    all_fails = parse_fails(path)
    for j in all_fails:
        if not j in raw_aborts['abort_list']:
            unknown_fails.append(j)
    weird_map = [j for j in raw_aborts['abort_list'] if not j in all_fails]
    if len(weird_map) > 0:
        print(path.rstrip('/') + '  WEIRD:   ' + str(weird_map))
    #assert(len(weird_map) == 0)
    return unknown_fails

def is_good(path):
    gl_filename = path.rstrip('/') + '/' + 'glideTester.log'
    fh = open(gl_filename, 'r')
    fd = fh.read()
    if 'KeyboardInterrupt' in fd: 
        sys.stderr.write('Found interupd in {}\n'.format(path))
        #return False 
    return True

def add_data(data, key, mapper):
    for itm in data:
        try:
            itm[key] = mapper(itm['wd'])
        except Exception, e:
            import sys
            sys.stderr.write("Got error adding {} to {}: {}\n".format(key, itm['wd'], str(e)))

def make_fz_data():
    wds = [{'wd' : d} for d in sys.argv[1:]]
    add_data(wds, 'grafana', parse_grafana)
    add_data(wds, 'params', parse_params)
    for itm in wds: 
        if itm['params']['concurrency'].strip() != '400':
            continue 
        itm['output_size'] = float(itm['params']['arguments']['flags']['--filecount']) * float(itm['params']['arguments']['flags']['--filesize'][:-1])
        if itm['output_size'] != 400.0:
            continue
        itm['fc'] = float(itm['params']['arguments']['flags']['--filecount'])
        itm['time'] = float(itm['grafana']['time'])
        print('{},{},{}'.format(itm['time'], itm['fc'], itm['output_size']))
def make_latency_data():
    wds = [{'wd' : d} for d in sys.argv[1:] if is_good(d)]
    add_data(wds, 'grafana', parse_grafana)
    add_data(wds, 'params', parse_params)
    add_data(wds, 'latency', parse_latency)
    add_data(wds, 'conlog_aborts', parse_aborts)
    add_data(wds, 'other_fails', parse_fail_unknowns)
    add_data(wds, 'shadow', parse_shadow_exceptions)
    add_data(wds, 'shadowcon', parse_conlog_shadow_exceptions)
        
    for itm in wds: 
        if not 'grafana' in itm or not 'params' in itm or not 'latency' in itm or not 'conlog_aborts' in itm or not 'other_fails' in itm: 
            sys.stderr.write("Error parsing {}.\n".format(itm['wd']))
            continue
        itm['output_size'] = float(itm['params']['arguments']['flags']['--filecount']) * float(itm['params']['arguments']['flags']['--filesize'][:-1])
        assert len(itm['latency']['rows']) == 1, "Failed at wd: " + itm['wd']
        itm['fc'] = float(itm['params']['arguments']['flags']['--filecount'])
        itm['time'] = float(itm['grafana']['time'])
        print('{},{},{},{},{},{},{},{}, {}'.format(itm['time'], itm['fc'], itm['output_size'], itm['latency']['last'], itm['conlog_aborts']['abort'], len(itm['other_fails']), len(itm['shadowcon']), (itm['shadow']['jwe']), itm['wd']))
make_latency_data()