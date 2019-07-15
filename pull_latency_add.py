
import subprocess 
import sys
import os
import time 

_OUT_FNAME = 'latency.txt'
def delay_to_ms(delaystr):
    reached_suffix = False 
    digits = []
    suffix = []
    for c in delaystr.strip():
        if not c in '0123456789.':
            reached_suffix = True 
        if reached_suffix:
            suffix.append(c)
        else: 
            digits.append(c)
    digits_str = ''.join(digits).strip('.')
    suffix_str = ''.join(suffix).lower()

    suffix_table = { 
        's' : 1000.0, 
        'ms' : 1.0, 
        'us' : 0.001, 
    }

    return float(digits_str) * suffix_table[suffix_str]


def run():
    wd = sys.argv[-1].rstrip('/') + '/'
    out_filename = wd + _OUT_FNAME

    if os.path.exists(out_filename):
        return 

    rawout = subprocess.check_output(['tc', 'qdisc', 'show', 'dev', 'ens785']).split('\n')
    delaylines = []
    for ln in rawout:
        if not 'delay' in ln: 
            continue 
        delaylines.append(delay_to_ms(ln.split(' ')[-1]))
    if len(delaylines) == 0:
        delaylines = [0]
    fh = open(out_filename, 'w') 
    for itm in delaylines:
        fh.write('{}, {}\n'.format(time.time(), itm))


if __name__ == "__main__":
    run()