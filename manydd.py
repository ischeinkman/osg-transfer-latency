#!/usr/bin/python2

""" Used for stress testing the sdsc-76 SSDs, mounted to /data1 and /data2. 

This is done by spawning `N` threads, each of which creates a file 500M in size
filled with 0 via `dd`. The program then sums up each thread's reported bitrate 
to report the total achievable write speed. 

"""

import subprocess 

N = 15

print('Starting speed test.')

cmd_format = "uptime && dd if=/dev/zero of=/data%d/tmp/file_%d.bin count=500M iflag=count_bytes && uptime"
zero_case = "uptime && dd if=/dev/zero of=/home/ilan/tmp/file_%d.bin count=500M iflag=count_bytes && uptime"
ddthreads = []
for idx in range(0, N):
    cmd = zero_case%(idx) if idx %3 == 0 else cmd_format%( idx%3, idx)
    ddthreads.append(subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, shell=True))

out = []
err = []

print('Waiting for threads to finish. ')

for idx in range(0, len(ddthreads)):
    cur_ot, cur_ed = ddthreads[idx].communicate()
    out.append(cur_ot)
    err.append(cur_ed)

def parse_uptime(uptime_line):
    parts = uptime_line.strip().split(' ')
    raw_time = parts[0]
    #raise RuntimeError('Raw time: ' + str(raw_time))
    hours, mins, seconds = raw_time.split(':')
    return 3600 * int(hours) + 60 * int(mins) + int(seconds)

def parse_speed(spd_line):
    parts = spd_line.strip().split(' ')
    mbs_part = parts[-2]
    return float(mbs_part)

def parse_out(fullout):
    print(fullout+'\n\n====\n\n')
    lns = fullout.split('\n')
    start = None 
    end = None 
    spd = None
    for ln in lns:
        if 'load average' in ln: 
            if start is None: 
                start = parse_uptime(ln)
            elif end is None: 
                end = parse_uptime(ln)
            else:
                assert(False)
        elif 'MB/s' in ln:
            assert(spd is None)
            spd = parse_speed(ln)
    return (start, end, spd)


print('Now parsing.')
time_borders = map(parse_out, out)
print('Now calcing.')

def approx_equal(a, b, delta):
    return abs(a - b) <= delta

def easy_calc(itms):
    starta, enda, _unused = itms[0]
    if any(map(lambda x : not approx_equal(x[0], starta, 2) or not approx_equal(x[1], enda, 2), itms)):
        #print("Bad: "+str(itms))
        #assert(False)
        pass
        #return (-1.0, -1.0) 
    return (sum(map(lambda x : x[2], itms)), 0.0)

ret = easy_calc(time_borders)
assert(ret is not None)
print("Speed: "+str(ret)+" MB/s")