#! /usr/bin/env python

import os
import time 
import random 
import sys 
import hashlib

def run(args):
    file_count = args.get('file_count') or 1 
    sleep_range = args.get('time_range') or 0.0
    secs_to_sleep = args['time'] + (random.random() - 0.5) * sleep_range
    file_size = args['file_size']

    start_time = time.time() 
    for _ in range(0, file_count):
        cur_name = make_random_file(file_size)
        cur_hash = make_file_md5(cur_name)
        print('File %s ||| MD5 %s'%(cur_name, cur_hash))
    sleep_until(start_time + secs_to_sleep)

def make_random_file(size, name = None):
    if name == None: 
        name = 'outfile_' + ''.join([chr( ord('a') + ord(a)% 26) for a in os.urandom(10)]) + '.txt'
    fh = open(name, 'w')
    fh.write(os.urandom(size))
    fh.close()
    return name

def make_file_md5(name):
    fh = open(name, 'r')
    data = fh.read() 
    fh.close()
    hasher = hashlib.md5()
    hasher.update(data)
    hsh = hasher.hexdigest()
    return hsh

def sleep_until(end_time):
    cur_time = time.time()
    while cur_time < end_time:
        cur_time = time.time()

def parse_args(args):
    retval = {}
    idx = 0
    while idx < len(args):
        cur_itm = args[idx]
        (cur_key, cur_val) = cur_itm.split('=', 1)
        if cur_key == '--filesize':
            retval['file_size'] = size_to_bytes(cur_val)
        elif cur_key == '--jobtime':
            retval['time'] = int(cur_val)
        elif cur_key == '--jobtimerange':
            retval['time_range'] = int(cur_val)
        elif cur_key == '--filecount':
            retval['file_count'] = int(cur_val)
        idx += 1
    return retval

def size_to_bytes(size_with_suffix):
    size_with_suffix = size_with_suffix.strip()
    num_part = ''.join(filter(lambda dig : dig.isdigit(), size_with_suffix))
    suffix_part = size_with_suffix[len(num_part):]
    if len(suffix_part) == 0:
        return int(num_part)
    multiplier = 1
    suffix_part = suffix_part[0].lower()
    if suffix_part == 'k':
        multiplier = 1024
    elif suffix_part == 'm':
        multiplier = 1024 * 1024
    elif suffix_part == 'g':
        multiplier = 1024 * 1024 * 1024
    return multiplier * int(num_part)


if __name__ == "__main__":
    argmap = parse_args(sys.argv[1:])
    run(argmap)