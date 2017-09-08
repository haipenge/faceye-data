#!/usr/bin/env python
# -*- coding: utf-8 -*-
#push data to hive-> hbase
#stat data from hdfs(hbase) with  hive -> mysql
#re push hive stat result to mysql
#author:haipenge
import os
import sys
import random
import shutil
import tempfile

#将mycat集群数据推送到hbase,为使用hive分析数据做准备.
class MySQL2HBase(object):
    def __init__(self):
        config =Config()
        #self.db_host=['10.1.5.105,10.1.5.250,10.1.5.126,10.1.5.127']
        self.db_host=config.get_db_host()
        #self.db_user_pw={'user':'prnp','password':'prnp'}
        self.db_user_pw=config.get_user_and_password()
        self.test_db_host=['10.1.5.126']
        self.test_db_user_pw={'user':'prnp','password':'Mysql@105'}
    #推送MySQL数据到HBase.
    def __to_hbase(self,host,db,user,password):
        cmd ='sqoop import --connect jdbc:mysql://'
        cmd +=host
        cmd +=':3306/'
        cmd +=db
        cmd +=' --table express_delivery_status'
        #cmd +=' --columns ID,GOODS_NAME,GOODS_PRICE'
        cmd +=' --hbase-table express_delivery_status'
        cmd +=' --column-family delivery'
        cmd +=' --hbase-row-key createDate,bill_number'
        #cmd +=' --hbase-create-table'
        cmd +=' --username "'+user+'"'
        cmd +=' --password "'+password+'"'
        cmd += ' --incremental lastmodified'
        cmd += ' --check-column createDate'
        cmd += ' --last-value "2017-08-01 00:00:00"'
        cmd += ' --append'
        cmd +=' --autoreset-to-one-mapper'
        #cmd +=' --direct'
        #cmd += ' --direct-split-size 3'
        #增加SQL条件
        #cmd +=' --where "id >= 5"'
        #使用多少个Mapper
        cmd += ' --m 3'
        #cmd += ' --split-by bill_number'
        print 'sqoop import cmd :',cmd
        os.system(cmd)
    #将Mycat集群的数据导入HBase
    def to_hbase(self):
        user=self.db_user_pw['user']
        password=self.db_user_pw['password']
        for i, host in enumerate(self.db_host):
            db_indexs = range(i * 25, (i + 1) * 25)
            for index in db_indexs:
                db='dbs'+str(index)
                self.__to_hbase(host, db, user, password)
                
#使用Hive分析HBase数据
class StatByHive(object):
    def __init__(self):
        print 'do stat by hive'
    def __prepare_create_table(self):
        sqls=[]
        #按企业，省市维度统计结果表(hive)
        create_sql_1="create table express_stat_res_2(exp_org_code string,province string,total int)  ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';"
        sqls.append(sql_1)
        return sqls
    def prepear(self):
        for sql in self.__prepare_create_table():
            cmd = 'hive -e "'+sql+'"'
            os.system(cmd)
            
    def __sqls__(self):
        sqls=[]
        #按企业 、省市维度统计HQL，并将统计结果写入hive table,express_stat_res_2
        sql_1="insert into table express_stat_res_2 select delivery['exp_org_code'] as exp_org_code,delivery['province'] as province,count(1) as total from express_delivery_status group by delivery['exp_org_code'],delivery['province'];"
        sqls.append(sql_1)
        return sqls
    #分析数据
    def stat(self):
        self.prepear()
        for sql in __sqls__():
            cmd = 'hive -e "'+sql+'"'
            os.system(cmd)
            
#将Hive的计算结果，导出到MySQL数据库
class Hive2MySQL(object):
    def __init__(self):
        print 'hive to mysql'
        self.db_host='10.1.5.250'
        self.db_name='nuc_stat'
        self.db_user='prnp'
        self.db_password='Mysql@105'
    #Hive导出 到mysql
    #table:mysql表名,如:stat_delivery
    #export_dir,hive在hdfs上的数据目录，如：/user/hive/warehouse/nuc.db/stat_delivery
    def export(self,table,export_dir):
        cmd='sqoop export '
        cmd += ' --connect jdbc:mysql://'
        cmd += self.db_host
        cmd += ':3306'
        cmd += '/'+self.db_name
        cmd += ' --username '+self.db_user
        cmd += ' --password '+self.db_password
        cmd += ' --table  '+table
        cmd += ' --export-dir '+export_dir
        cmd += " --input-fields-terminated-by '\t'"
        os.system(cmd)
        
class Toy(object):
    def __init__(self):
        print 'I a little toy.'

#统一配置
class Config(object):
    def __init__(self):
        print 'start 2 config system'
        self.db_host=['10.1.5.105','10.1.5.250','10.1.5.126','10.1.5.127']
        self.db_user_pw={'user':'prnp','password':'prnp'}
        self.test_db_host=['10.1.5.126']
        self.test_db_user_pw={'user':'prnp','password':'Mysql@105'}
    def get_db_host(self):
        return self.db_host
    def get_user_and_password(self):
        return self.test_db_user_pw
        
if __name__ == '__main__':
    #mysql_2_hbase=MySQL2HBase()
    #mysql_2_hbase.to_hbase()
    #stat_by_hive=StatByHive()
    #stat_by_hive.stat()
    hive_2_mysql=Hive2MySQL()
    #将hive统计结果导出到MySQL
    hive_2_mysql.export('express_stat_res_2', '/user/hive/warehouse/nuc.db/express_stat_res_2')