#!/bin/bash
######################################
#Desc:push mysql data to hdfs
#Author:haipenge
#Date:2017.06.15
######################################
HOST='10.12.12.141'
PORT='3306'
DB='tongjidb'
USER='prnp'
PASSWORD='prnp'
HIVE_DB='hive_nuc'

#push express_delivery_staus_interface to hdfs
#--target-dir /hive/nuc
sqoop import -m 4 --connect jdbc:mysql://$HOST:$PORT/$DB --username $USER --password $PASSWORD \
--table express_delivery_status_interface \
--hive-import \
--hive-database $HIVE_DB \
--hive-overwrite \
--create-hive-table \
--hive-table express_delivery_status_interface \
--direct \
--split-by exp_org_code
--delete-target-dir

#快件表 mysql 2 hdfs(hive-hdfs)
sqoop import -m 4 --connect jdbc:mysql://$HOST:$PORT/$DB --username $USER --password $PASSWORD \
--table express_delivery_status \
--hive-import \
--hive-database $HIVE_DB \
--hive-overwrite \
--create-hive-table \
--hive-table express_delivery_status \
--direct \
--split-by exp_org_code
--delete-target-dir

#push ec_indivuser to hdfs
#--target-dir /hive/nuc
sqoop import -m 1 --connect jdbc:mysql://$HOST:$PORT/$DB --username $USER --password $PASSWORD \
--table ec_indivuser \
--hive-import \
--hive-database $HIVE_DB \
--hive-overwrite \
--create-hive-table \
--hive-table ec_indivuser \
--direct \
--delete-target-dir