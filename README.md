# OSG Transfer Time Tests

## Introduction

This repository is the code used to run the OSG transfer time research; it does *not,* however,
contain the actual data (since that would amount to hundreds of gigabytes). What follows is an
explanation of data collection and run methodoligies, including explainations of what each particular
file and script is for.

## Scripts

### `manydd.py`

This script was ran to determine the maximum possible write speed atainable with our current test host.
This was done by spanning multiple threads, each using a `dd` command to write a zero-filled file to one
of the disks used to by the tests themselves. The output of `dd` was then parsed for each thread and the
recorded rates summed before being output to determine the total write speed reached.

### `prescript.py`

This script copies the parameters, glideTester config, and job script used to the actual run instance's output
folder for record keeping purposes. This is ran as a GlideTester prescript value to gurantee the data is recorded
regardless of run status or completion.  

### `runner.py`

This is the Python 2 script sent as the actual job for the Glideins. It contains parameters for
the number of files the job generates, the size of each file, the time to use as the job start,
and the mean and range to use to randomly generate a sleep time (though, currently, all runs have had
their range parameters set to 0, IE they all attempt to run the exact same amount of time). The script,
in order:

1. Determintes the job start, if one was not passed.

2. Generates the required number of files at the required size, giving each a random name and filling it with random ASCII text.

3. Calculates each file's MD5 hash, printing the name and hash to the stdout.

4. Waits until the time is job sleep time + effective start time before terminating.

### `postscript.sh`

The file passed as the GlideTester `postscript` value. Currently it just proxies the working directory to the python scripts
listed below before also copying the condor `XferStatsLog` into the current run's working directory.

### `datagen.py`

This file contains a variety of utility scripts and functions for data collection, reduction, and visualization. In addition,
when ran as a script as part of the `postscript.sh`, it loops through the job output folders, parses the stdout to determine
what each random output file's *expected* MD5 hash is, and then calculates the MD5 hashes of the files actually transfered back.
It then constructs a CSV file in the output folder containing the job's concurrency-run-procid value, and the number of files that
had an MD5-mismatch.

### `pull_grafana.py`

This file queries our local statistics InfluxDB instance for the values of `JobAccumPostExecuteTime` within a range encompassing
an entire run (with extra time padded at both ends to gurantee correct data collection). The data is then summarized to only include
rows representing the first timestamp a distinct value of `JobAccumPostExecuteTime` appeared within that range, and then the rows are
outputted to a file. Since this test was the only one being ran on the given host, this should produce exactly 2 distinct rows
representing the schedd's `JobAccumPostExecuteTime` value before and after the run, so that the difference between them can be used
to determine the total time spent transfering files.

### `pull_transfer.py`

This file queries another local InfluxDB instance to record the raw bytes-per-second transfer rate of the host used for these tests within a run's
time range, outputting the results to a file. Note that currently this script requires an environment variable, `GAUTH`, to be set with a valid value
to use as the InfluxDB request's `Authorization` header; for valid values, contact the repository owner.

### `pull_latency_add.py`

Uses `tc` to determine the amount of latency artifically added to the network, in milliseconds. Note that older runs may not have a `latency.txt` file;
these runs were done before testing latency in general, and as such have an artificial latency value of 0.
