#!/bin/bash
mvn clean package -D maven.test.skip=true
scp target/*.jar prnp@10.12.12.140:/home/prnp/hadoop/mr