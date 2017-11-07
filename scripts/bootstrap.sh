#!/bin/bash
mars='faceye-data-0.0.1-SNAPSHOT.jar'
#执行
./bin/spark-submit $mars spark

#启动stat-company消费类
java -jar $mars consumer-stat-company &
#启动stat-record消费类
java -jar $mars consumer-stat-record & 

java -jar $mars stream-express-delivery-topic &