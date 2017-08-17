#!/usr/bin/env python
# -*- coding: utf-8 -*-
# create multi dbs on different servers
# author:haipenge
# Date:2917.08.08
import os
import sys
import random
import shutil
import tempfile

class SchemaTools(object):
    def __init__(self):
        self.servers = ['10.11.100.39', '10.11.100.40', '10.11.100.41', '10.11.100.42']
        self.test_servers = ['10.1.5.105', '10.1.5.250', '10.1.5.126', '10.1.5.127']
        self.dev_servers = ['10.12.12.140', '10.12.12.141', '10.12.12.142', '10.12.12.143']
        self.user_and_password = {'user':'prnp', 'password':'prnp'}
        self.dev_user_and_password = {'user':'prnp', 'password':'prnp'}
        self.test_user_and_password = {'user':'prnp', 'password':'Mysql@105'} 
    def __exec_db_cmds(self, host, user, password, dbCmd):
        print 'user is ', user
        cmd = 'mysql -u ' + user + ' -p' + password + ' -h ' + host + ' -e "' + dbCmd + '"'
        os.system(cmd)

    def __get_user(self, set_env):
        user = 'prnp'
        if set_env == 'test':
            user = self.test_user_and_password['user']
        elif set_env == 'dev':
            user = self.dev_user_and_password['user']
        elif set_env == 'prod':
            user = self.user_and_password['user']
        else:
            print 'env is not exist.'
        return user
    def __get_password(self, set_env):
        password = 'prnp'
        if set_env == 'test':
            password = self.test_user_and_password['password']
        elif set_env == 'dev':
            password = self.dev_user_and_password['password']
        elif set_env == 'prod':
            password = self.user_and_password['password']
        else:
            print 'set env is not exist.'
        return password

    # 创建生产库，库名：dbs0-99,四台，每台25片
    def __generate_db(self, user, hosts):
        u = user['user']
        password = user['password']
        for i, ip in enumerate(hosts):
            print i, ip
            db_indexs = range(i * 25, (i + 1) * 25)
            for i in db_indexs:
                command = 'create database dbs' + str(i)
                print command
                self.__exec_db_cmds(ip, u, password, command)
        cmd = 'create database nuc_pub;'
        self.__exec_db_cmds(hosts[0], u, password, cmd)
        cmd = 'create database nuc_stat;'
        self.__exec_db_cmds(hosts[1], u, password, cmd)
        cmd = 'create database nuc_indivuser;'
        self.__exec_db_cmds(hosts[2], u, password, cmd)
    # 删除测试库
    def __drop_dbs(self, user, hosts):
        u = user['user']
        password = user['password']
        for i, ip in enumerate(hosts):
            db_indexes = range(i * 25, (i + 1) * 25)
            for index in db_indexes:
                cmd = 'drop database dbs' + str(index)
                self.__exec_db_cmds(ip, u, password, cmd)
        cmd = 'drop database nuc_pub;'
        self.__exec_db_cmds(hosts[0], u, password, cmd)
        cmd = 'drop database nuc_stat;'
        self.__exec_db_cmds(hosts[1], u, password, cmd)
        cmd = 'drop database nuc_indivuser;'
        self.__exec_db_cmds(hosts[2], u, password, cmd)
    def generate(self, set_env):
        if set_env == 'test':
            self.__generate_db(self.test_user_and_password, self.test_servers)
        elif set_env == 'dev':
            self.__generate_db(self.dev_user_and_password, self.dev_servers)
        else:
            self.__generate_db(self.user_and_password, self.servers)
    def drop(self, set_env):
        if set_env == 'test':
            self.__drop_dbs(self.test_user_and_password, self.test_servers)
        elif set_env == 'dev':
            self.__drop_dbs(self.dev_user_and_password, self.dev_servers)
        elif set_env == 'prod':
            self.__drop_dbs(self.user_and_password, self.servers)
        else:
            print('输入参数错误，环境参数范围：[test,dev,prod]')
    def execSql(self, set_env, sql):
        user = self.__get_user(set_env)
        password = self.__get_password(set_env)
        if set_env == 'dev':
            for host in self.dev_servers:
                self.__exec_db_cmds(host, user, password, sql)
        elif set_env == 'test':
            for host in self.test_servers:
                self.__exec_db_cmds(host, user, password, sql)
        elif set_env == 'prod':
            for host in self.servers:
                self.__exec_db_cmds(host, user, password, sql)
        else:
            print 'env is not exist'

if __name__ == '__main__':
    tools = SchemaTools()
    action = raw_input('动作:[generate,drop,execSql]')
    set_env = raw_input('初始化环境[test,dev,prod]:')
    if action == 'generate':
        tools.generate(set_env)
    elif action == 'drop':
        tools.drop(set_env)
    elif action == 'execSql':
        sql = raw_input('请输入要执行的SQL[92]:');
        if sql != '':
            print sql
            tools.execSql(set_env, sql)
        else:
            print 'sql不能为空.'
    else:
        print '动作:', action , '不存在。动作范围:[generate,drop]'
