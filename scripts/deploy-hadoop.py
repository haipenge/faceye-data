#!/usr/bin/env python
# -*- coding: utf-8 -*-
#Distrbute hadoop
#author:haipenge
#Date:2017.07.21
import os
import sys
import random
import shutil
import tempfile

class Distribute(object):
	def __init__(self):
		self.slave_ips=['10.12.12.141','10.12.12.142','10.12.12.143']
		self.hadoop_version='2.7.3'
		self.hbase_version='1.3.1'
		self.spark_version='spark-2.2.0-bin-hadoop2.7'
		self.root_dir='/home/prnp/hadoop'
		self.hadoop_user='prnp'
	def dist(self):
		for ip in self.slave_ips:
			print ip
			source_file=self.root_dir+'/hadoop-'+self.hadoop_version+'.tar.gz'
			target_dir=self.hadoop_user+'@'+ip+':'+self.root_dir+"/"
			os.system('scp '+source_file+' '+target_dir)
			os.system('ssh '+self.hadoop_user+'@'+ip+ ' \"cd '+self.root_dir+';tar -zxvf hadoop-'+self.hadoop_version+'.tar.gz\"')
	def dist_spark(self):
		for host in self.slave_ips:
			source_file=self.root_dir+'/'+self.spark_version+'.tgz'
			target_dir=self.hadoop_user+'@'+host+':'+self.root_dir+'/'
			os.system('scp '+source_file+' '+target_dir)
			os.system('ssh '+self.hadoop_user+'@'+host+' \"cd '+self.root_dir+';tar -zxvf '+self.spark_version+'.tgz"')
	def dist_spark_conf(self):
		spark_conf_dir=self.root_dir+'/'+self.spark_version+'/conf'
		for host in self.slave_ips:
			os.system('scp -r '+spark_conf_dir+'/* '+self.hadoop_user+'@'+host+':'+spark_conf_dir)
	def dist_hbase(self):
		for host in self.slave_ips:
			source_file=self.root_dir+'/hbase-'+self.hbase_version+'-bin.tar.gz'
			target_dir=self.hadoop_user+"@"+host+":"+self.root_dir+"/"
			os.system('scp '+source_file+" " +target_dir)
			os.system('ssh '+self.hadoop_user+'@'+host+ ' \"cd '+self.root_dir+';tar -zxvf hbase-'+self.hbase_version+'-bin.tar.gz\"')
	def dist_hbase_conf(self):
		hbase_conf_dir=self.root_dir+'/hbase-'+self.hbase_version+'/conf'
		for host in self.slave_ips:
			os.system('scp -r '+hbase_conf_dir+'/* '+self.hadoop_user+'@'+host+':'+hbase_conf_dir)
    #Dist hadoop config to slave.
	def dist_conf(self):
		for ip in self.slave_ips:
			conf_dir=self.root_dir+'/hadoop-'+self.hadoop_version+'/etc/'
			native_lib_dir=self.root_dir+'/hadoop-'+self.hadoop_version+'/lib/native/'
			hbase_conf_dir=self.root_dir+'/hbase-'+self.hbase_version+'/conf'
			print 'scp -r '+conf_dir+'* '+ self.hadoop_user+'@'+ip+':'+conf_dir
			print 'scp -r '+native_lib_dir+'* '+ self.hadoop_user+'@'+ip+':'+native_lib_dir
			os.system('scp -r '+conf_dir+'* '+ self.hadoop_user+'@'+ip+':'+conf_dir)
			os.system('scp -r '+native_lib_dir+'* '+ self.hadoop_user+'@'+ip+':'+native_lib_dir)
			os.system('scp -r '+hbase_conf_dir+'/hbase-site.xml '+self.hadoop_user+'@'+ip+':'+conf_dir)
	def re_source_profile(self):
		for ip in self.slave_ips:
			cmd='ssh '+self.hadoop_user+'@'+ip+' \"source /etc/profile\"'
			self.execLinuxCmd(cmd)
	def execLinuxCmd(self,cmd):
		os.system(cmd)


if __name__=='__main__':
	distribute=Distribute()
	command=raw_input('1:Dist hadoop version to slave.\n2:Dist hadop conf,native lib to slave.\n3:Do source  etc profile on slave.\n4:Dist hbase.\n5:Dist hbase conf\n6:Dist spark.\n7.Dist spark conf.\nChoose[1,2,3,4,5,6,7]:')
	if command=='1':
		distribute.dist()
	elif command == '2':
		distribute.dist_conf()
	elif command == '3':
		distribute.re_source_profile()
	elif command == '4':
		distribute.dist_hbase()
		distribute.dist_hbase_conf()
	elif command == '5':
		distribute.dist_hbase_conf()
	elif command == '6':
		distribute.dist_spark()
	elif command == '7':
		distribute.dist_spark_conf()
	else:
		print 'Erro input'

