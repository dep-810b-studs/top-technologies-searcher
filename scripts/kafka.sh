kafka-topics --zookeeper stud-cdh1:2181 --list

kafka-topics --zookeeper stud-cdh1:2181 --create --topic test1 --partitions 4 --replication-factor 2

kafka-console-consumer --zookeeper stud-cdh1:2181 --topic test1

kafka-console-producer --broker-list stud-cdh1:9092,stud-cdh2:9092 --topic test1