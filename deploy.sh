 mvn install -DskipTests
 scp -r target/alluxio-integration-myyarn-2.1.2-jar-with-dependencies.jar root@ns1:/home/console/hadoop_client/bin/
 scp -r bin/*.sh root@ns1:/home/console/hadoop_client/bin
