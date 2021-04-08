#!/bin/bash
./runhosts.sh
cd /usr/local/hadoop/sbin
./stop-all.sh
rm -r /usr/local/hadoop/tmp/*
../bin/hadoop namenode -format
./start-all.sh
../bin/hadoop dfsadmin -report
