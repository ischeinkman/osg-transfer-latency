import sys 
import time



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
            entry = {}
            timestamp_raw = line[:18]
            entry['kind'] = prefix.split(' ')[-1]
            entry['timestamp'] = parse_timestamp(timestamp_raw)
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
    retval = {'inf' : 0, 'mb_s' : 0, 'kb_s' : 0, 'b_s' :0}
    for ent in rows: 
        speed = ent['bytes_per_second']
        if speed == 'INF':
            retval['inf'] += 1
        elif speed > (1024 * 1024):
            retval['mb_s'] += 1
        elif speed > 1024:
            retval['kb_s'] += 1
        else: 
            retval['b_s'] += 1
    return retval 


if __name__ == "__main__":
    tv = parse_xfer_log(sys.argv[1])
    add_range_info(tv)
    add_speed_info(tv)
    by_dest = data_by_dest(tv, False)
    for dest in by_dest:
        raw = by_dest[dest]
        print('%s => %s'%(str(dest), str(speed_categorize(raw))))
    