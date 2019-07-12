#!/usr/bin/python 

import sys 
import shutil
needed_files = ['parameters.cfg', 'runner.py', 'glideTester.cfg']
wd = sys.argv[-1]
base = wd.rstrip('/').rsplit('/', 1)[0]
for fl in needed_files:
    basefl = base + '/' + fl 
    destfl = wd + '/' + fl 
    shutil.copy(basefl, destfl)

