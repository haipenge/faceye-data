#!/usr/bin/env python
# -*- coding: utf-8 -*-
# auto bakcup mysql
# author:haipenge
# Date:2017.09.19
import os
import sys
import random
import shutil
import tempfile
import time
import datetime

#邮政项目现网数据库备份 
#脚本目录：prnp@10.11.100.230:/home/prnp/shell/mysql_auto_backup.py
#本机备份目录:10.11.100.230:/home/prnp/back
#备份规则 ：2017.9.18日备份全库
#2017.9.18日后,每日凌晨备份前一日凌晨至今天凌晨数据
#异备
#备机：10.11.100.231
#异备目录：/home/prnp/back
class SchemaTools(object):
    def __init__(self):
        #待备份表
        self.__tables=['ec_indivuser','courier','orguser']
        #用户
        self.__user={'user':'prnp','password':'prnp'}
        #备份目录
        self.__backdir='/home/prnp/back/'
        #今天 
        self.__now_time = datetime.datetime.now()
        #昨天
        self.__yes_time = self.__now_time + datetime.timedelta(days=-1)
        #format
        self.__yes_fmt = self.__yes_time.strftime("%Y-%m-%d")
        self.__yes_fmt = self.__yes_fmt+" 00:00:00"
        self.__today=time.strftime("%Y-%m-%d", time.localtime())
        self.__today=self.__today+" 00:00:00"
        #备份文件名
        self.__backup_file=self.__yes_time.strftime("%Y-%m-%d")
    #本机备份
    def local_backup(self):
        for table in self.__tables:
            cmd="mysqldump -uprnp -pprnp -c --skip-add-locks --skip-add-drop-table "
            cmd+="nuc_indivuser "
            cmd+=table
            if table == 'orguser':
                cmd+=' --where="CheckTime>=\''
            else:
                cmd+=' --where="CreateTime>=\''
            cmd+=self.__yes_fmt
            cmd+='\''
            if table =='orguser':
                cmd+=' and CheckTime <\''
            else:
                cmd+=' and CreateTime <\''
            cmd+=self.__today
            cmd+='\'"'
            cmd+=' |gzip >'
            cmd+=self.__backdir
            cmd+=table
            cmd+='_bak_'
            cmd+=self.__backup_file
            cmd+='.gz'
            print cmd
            os.system(cmd)
    #异机备份
    def remote_backup(self):
        cmd='scp /home/prnp/back/*'
        cmd+=self.__backup_file
        cmd+='.gz'
        cmd+=' prnp@10.11.100.231:/home/prnp/back/'
        os.system(cmd)

if __name__ == '__main__':
    tools=SchemaTools()
    tools.local_backup()
    tools.remote_backup()