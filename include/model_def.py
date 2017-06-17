#!/usr/local/bin/python
# -*- coding: utf-8 -*-
"""
数据处理
@author tonyfeng
@date  2016.11.9 
"""
import os
import MySQLdb
import config


def mysql_connect(chost = config.db_host,cuser = config.db_user,cpasswd = config.db_passwd,cdb = config.db_name):
    '''
    实例化mysql连接
    @param chost 主机名称
    @param cuser 用户
    @param cpasswd 密码
    '''
    conn = MySQLdb.connect(host = chost,user = cuser,passwd = cpasswd)
    conn.select_db(cdb)
    conn.set_character_set('utf8')
    cur = conn.cursor()
    return conn,cur

def format_data(data,desc):
    '''
    格式化数据输出
    @param data 源数据
    @param desc 表结构
    @return list
    '''
    if not data:
        return data

    out_data = []
    for info in data:
        temp_data = {}
        for i in range(0,len(info)):
            temp_data[desc[i][0]] = str(info[i])
        out_data.append(temp_data)
    return out_data

def query_data(cur,sql):
    '''
    查询数据
    @param cur 连接句柄
    @param sql
    @return dict
    '''
    cur.execute(sql)
    data = cur.fetchall()
    desc = cur.description
    data = format_data(data,desc)
    return data

def query_count(cur,sql):
    '''
    计算行数
    @param cur 连接句柄
    @param sql
    @return int
    '''
    cur.execute(sql)  
    result = cur.fetchone()  
    totalRowsNum = 0  
    if result != None:  
        totalRowsNum = int(result[0])  
    return totalRowsNum  
	



