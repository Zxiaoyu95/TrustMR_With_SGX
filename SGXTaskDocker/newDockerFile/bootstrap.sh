#!/bin/bash

: ${HADOOP_PREFIX:=/usr/local/hadoop}

$HADOOP_PREFIX/etc/hadoop/hadoop-env.sh

rm /tmp/*.pid

# installing libraries if any - (resource urls added comma separated to the ACP system variable)
cd $HADOOP_PREFIX/share/hadoop/common ; for cp in ${ACP//,/ }; do  echo == $cp; curl -LO $cp ; done; cd -

/usr/sbin/sshd  && \
ssh-keygen -t dsa -P ""  && \
ssh-keygen -t rsa -P ""  && \
ssh-keygen -t ecdsa -P ""  && \
ssh-keygen -t ed25519 -P ""  && \
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys && \
cat ~/.ssh/id_dsa.pub >> ~/.ssh/authorized_keys && \
chmod 600 ~/.ssh/authorized_keys && \
chmod 700 ~/.ssh/

# altering the core-site configuration
sed s/HOSTNAME/$HOSTNAME/ /usr/local/hadoop/etc/hadoop/core-site.xml.template > /usr/local/hadoop/etc/hadoop/core-site.xml


$HADOOP_PREFIX/sbin/start-dfs.sh
$HADOOP_PREFIX/sbin/start-yarn.sh

if [[ $1 == "-d" ]]; then
  while true; do sleep 1000; done
fi

if [[ $1 == "-bash" ]]; then
  /bin/bash
fi
~    
