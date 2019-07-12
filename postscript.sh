python2 pull_grafana.py $1
python2 pull_transfer.py $1
python2 datagen.py --md5 $1
cp /var/log/condor/XferStatsLog $1