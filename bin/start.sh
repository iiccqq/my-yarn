#!/bin/bash
#tar zxf myyarn.tar.gz
#python -m SimpleHTTPServer 7000 &

echo "=========start.sh am master or worker============"
java -cp ".:/home/console/hadoop_client/conf:./*" alluxio.yarn.ApplicationMaster
#java -agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=5005 -cp "/home/console/hadoop_client/conf:.:./*"
# alluxio.yarn.ApplicationMaster
