import sys 
import time
import hashlib
import os

def parse_timestamp(raw):
    month = int(raw[0:2])
    day = int(raw[3:5])
    year = 2000 + int(raw[6:8])

    hour = int(raw[9:11])
    minute = int(raw[12 : 14])
    second = int(raw[15 : 17])
    return "%.4d-%.2d-%.2d %.2d:%.2d:%.2d"%(year, month, day, hour, minute, second)


def parse_xfer_log(logpath):
    retval = []
    with open(logpath, 'r') as fh:
        for line in fh:
            prefix = ''
            if 'File Transfer Upload' in line: 
                prefix = 'File Transfer Upload'
            elif 'File Transfer Download' in line:
                prefix = 'File Transfer Download'
            else: 
                continue
            is_peer = 'peer' if '(peer stats from starter)' in line else 'client'
            entry = {}
            timestamp_raw = line[:18]
            entry['kind'] = prefix.split(' ')[-1]
            entry['timestamp'] = parse_timestamp(timestamp_raw)
            entry['ClusterId'] = line.split(' ')[3][1:-2]
            entry['povsource'] = is_peer
            data_start_idx = line.index(prefix) + len(prefix) + 1
            data_components = line[data_start_idx : ].replace('\n', '').strip().split(' ')
            pairs_count = int(len(data_components)/2)
            for idx in range(0, pairs_count):
                key = data_components[2 * idx][:-1]
                val = data_components[2 * idx + 1]
                assert "Key rept: %s"%(key), key not in entry
                entry[key] = val
            retval.append(entry)
    return retval 

def add_range_info(rows):
    for row in rows:
        dt = float(row['seconds'])
        end_time_tuple = time.strptime(row['timestamp'], '%Y-%m-%d %H:%M:%S')
        unix_end_time = time.mktime(end_time_tuple)
        unix_start_time = unix_end_time - dt
        row['starttime'] = unix_start_time
        row['endtime'] = unix_end_time

def add_speed_info(rows):
    for row in rows:
        dt = float(row['seconds'])
        if dt == 0:
            row['bytes_per_second'] = 'INF'
            continue
        db = float(row['bytes'])
        row['bytes_per_second'] = db/dt 

def data_by_jobs(rows):
    retval = {}
    for ent in rows: 
        if not ent['JobId'] in retval:
            retval[ent['JobId']] = []
        retval[ent['JobId']].append(ent)
    return retval

def data_by_dest(rows, with_direction=True):
    retval = {}
    for ent in rows: 
        key = ent['dest']
        if with_direction:
            prefix = '-> ' if ent['kind'].lower()[0] == 'u' else '<- '
            key = prefix + key
        if not key in retval:
            retval[key] = []
        retval[key].append(ent)
    return retval

def speed_categorize(rows):
    retval = {'inf' : 0, 'mb_s' : 0, 'kb_s' : 0, 'b_s' :0, 'infj' : [], 'j' : []}
    for ent in rows: 
        speed = ent['bytes_per_second']
        job =ent['ClusterId'] + '.' + ent['JobId']
        retval['j'].append(job)
        if speed == 'INF':
            retval['inf'] += 1
            retval['infj'].append(job)
        elif speed > (1024 * 1024):
            retval['mb_s'] += 1
        elif speed > 1024:
            retval['kb_s'] += 1
        else: 
            retval['b_s'] += 1
        if len(ent['JobId'].strip()) == 0:
            retval['nj'] += 1
    return retval 

def not_initial_send(entry):
    return not (entry['povsource'] == 'client' and entry['kind'] == 'Upload')


def make_file_md5(name):
    fh = open(name, 'r')
    data = fh.read() 
    fh.close()
    hasher = hashlib.md5()
    hasher.update(data)
    hsh = hasher.hexdigest()
    return hsh

def md5_check(outdir):
    dirlist = os.listdir(outdir)
    stdout_list = [outdir + '/' + itm for itm in dirlist if itm.endswith('.out')]
    assert(len(stdout_list) == 1)
    stdout_fh = open(stdout_list[0], 'r')

    hashes = {}
    lines = stdout_fh.readlines()
    for ln in lines: 
        if not ln.startswith('File ') or not ' ||| MD5 ' in ln: 
            continue 
        (fname, expectedhash) = ln[len('File '):].split(' ||| MD5 ', 1)
        hashes[fname] = expectedhash
    
    outfiles = [fl for fl in dirlist if fl in hashes]
    assert(len(outfiles) == len(hashes))

    misses = 0
    matches = 0
    for fl in outfiles:
        actual_hash = make_file_md5(outdir + '/' + fl)
        expected_hash = hashes[fl]
        if actual_hash != expected_hash:
            misses += 1
        else:
            matches += 1
    return (matches, misses)


def parse_conlog(raw_data):
    retval = []
    cur_ent = {}
    lines = raw_data.split('\n')
    for ln in lines: 
        ln = ln.strip()
        if ln == '...':
            if not len(cur_ent) == 0:
                retval.append(cur_ent)
            cur_ent = {}
            continue
        elif not ' = ' in ln: 
            continue 
        kvl = ln.split(' = ', 1)
        if not len(kvl) == 2:
            print('Bad ln: %s'%(ln))
        key_raw, val_raw = kvl
        key = key_raw.strip()
        val = val_raw.strip().strip('"')
        assert not key in cur_ent
        cur_ent[key] = val
        if key == 'EventTime':
            cur_ent['unixtime'] = time.mktime(parse_conlog_timestamp(val))
    return retval

def parse_conlog_timestamp(ts_raw):
    fmt = '%Y-%m-%dT%H:%M:%S'
    return time.strptime(ts_raw, fmt)

def combine_data_by_job(conlog_data, xfer_data):
    retval = {}
    xfer_map = data_by_jobs(xfer_data)
    for ent in conlog_data:
        if not 'Cluster' in ent or not 'Proc' in ent:
            print('Bad ent!')
            print('Keys: %s'%(str(ent.keys())))
        entkey = ent['Cluster'] + '.' + ent['Proc']
        if entkey in retval:
            retval[entkey]['conlog'].append(ent)
        else: 
            retval[entkey] = {
                'xfer' : xfer_map.get(entkey) or [],
                'conlog' : [ent],
            }
    for ent in conlog_data:
        entkey = ent['Cluster'] + '.' + ent['Proc']
        dt = retval[entkey]
        if not (len(dt['xfer']) > 0 or dt['conlog'][-1]['MyType']  == 'JobAbortedEvent'):
            print('Bad job %s ended with event %s'%(entkey, dt['conlog'][-1]['MyType']))
    return retval

def summarize_aborts(comb_data):
    retval = {'aborts' : []}
    for key in comb_data:
        if len(comb_data[key]['xfer']) == 0:
            retval['aborts'].append(key)
        else: 
            retval[key] = comb_data[key]
    return retval 

def jid_sorter(a, b):
    aj_raw, ac_raw = a.split('.')
    aj = int(aj_raw)
    ac = int(ac_raw)
    bj_raw, bc_raw = b.split('.')
    bj = int(bj_raw)
    bc = int(bc_raw)
    if aj != bj:
        return aj - bj 
    else: 
        return ac - bc


def argparser(args):
    flags = {}
    noflag = []
    for itm in args:
        if not itm.startswith('--'):
            noflag.append(itm)
        elif '=' in itm: 
            k,v = itm[2:].split('=', 1)
            flags[k] = v 
        else: 
            flags[itm[2:]] = True
    return (flags, noflag)

def xfer_log_run(flags, noflag):
    fname = noflag[0] if flags['xfer'] is True else flags['xfer']
    tv = parse_xfer_log(fname)
    add_range_info(tv)
    add_speed_info(tv)
    by_job = data_by_jobs(filter(not_initial_send, tv))
    for job in by_job:
        raw = by_job[job]
        cont = '\n\t'.join(map(str, raw))
        print('%s => %s\n\n'%(str(job), str(cont)))

def con_log_run(flags, noflag):
    fname = noflag[0] if flags['conlog'] is True else flags['conlog']
    raw_data = ''
    with open(fname, 'r') as fh:
        raw_data = fh.read()
    tv = parse_conlog(raw_data)
    for ent in tv: 
        print(ent)

def make_combdata(xfer_fname, conlog_fname):
    xfer_data =filter(not_initial_send, parse_xfer_log(xfer_fname))
    
    conlog_raw_data = ''
    with open(conlog_fname, 'r') as fh:
        conlog_raw_data = fh.read()
    conlog_data = parse_conlog(conlog_raw_data)

    add_range_info(xfer_data)
    add_speed_info(xfer_data)

    return summarize_aborts(combine_data_by_job(conlog_data, xfer_data))


def comb_run(flags, noflag):
    comb_data = make_combdata(flags['xfer'], flags['conlog'])
    for key in comb_data:
        if key == 'aborts':
            continue
        print('==============\n\n%s:\n\n'%(key))
        conlog_cont = '\n\t\t'.join(map(str, comb_data[key]['conlog']))
        print('\tconlog:\n\t\t%s\n\n'%(str(conlog_cont)))
        xfer_cont = '\n\t\t'.join(map(str, comb_data[key]['xfer']))
        print('\txfer:\n\t\t%s\n\n'%(str(xfer_cont)))
    print('==============\n\n%s (%d):\n\n'%('aborts', len(comb_data['aborts'])))
    print('%s\n\n'%(str(comb_data['aborts'])))
    return comb_data

def timecheck_run(flags, noflag):
    goal_end = float(flags['endgoal'])
    comb_data = make_combdata(flags['xfer'], flags['conlog'])
    retval = {
        'JobId' : [],
        'PeerSeconds' : [],
        'PeerDiff' : [],
        'ClientSeconds' : [],
        'ClientDiff' : [], 
        'ValidSource' : [],
        'TermFullDiff' : [],
    }

    for j in comb_data:
        if j == 'aborts':
            continue
        JobId = j 
        if not type({}) == type(comb_data[j]):
            print('Bad j: %s => %s'%(str(j), str(type(comb_data[j]))))
        peerent = [ent for ent in comb_data[j]['xfer'] if ent['povsource'] == 'peer'][0]
        PeerSeconds = float(peerent['seconds'])
        PeerDiff = peerent['endtime'] - goal_end
        clientent = [ent for ent in comb_data[j]['xfer'] if not ent['povsource'] == 'peer'][0]
        ClientSeconds = float(clientent['seconds'])
        ClientDiff = clientent['endtime'] - goal_end
        ValidSource = ''
        if clientent['starttime'] - goal_end > peerent['starttime'] - goal_end:
            ValidSource = 'peer'
        else: 
            ValidSource = 'client'
        termed = comb_data[j]['conlog'][-1]
        TermFullSeconds = termed['unixtime'] - goal_end
        retval['JobId'].append(JobId)
        retval['PeerSeconds'].append(PeerSeconds)
        retval['PeerDiff'].append(PeerDiff)
        retval['ClientDiff'].append(ClientDiff)
        retval['ClientSeconds'].append(ClientSeconds)
        retval['ValidSource'].append(ValidSource)
        retval['TermFullDiff'].append(TermFullSeconds)
    print(retval)
    return retval
        



if __name__ == "__main__":
    flags, noflag = argparser(sys.argv[1:])
    if 'xfer' in flags and 'conlog' in flags and 'endgoal' in flags:
        timecheck_run(flags, noflag)
    elif 'xfer' in flags and 'conlog' in flags:
        comb_run(flags, noflag)
    elif 'xfer' in flags:
        xfer_log_run(flags, noflag)
    elif 'conlog' in flags:
        con_log_run(flags, noflag)