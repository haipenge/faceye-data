#!/bin/bash
hadoop fs -rmr /db
#hadoop fs -mkdir /lib/
#hadoop fs -put /Users/apple/.m2/repository/mysql/mysql-connector-java/5.1.42/mysql-connector-java-5.1.42.jar /lib/
hadoop jar /work/project/faceye-data/target/faceye-data-0.0.1-SNAPSHOT.jar stock