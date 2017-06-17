#!/usr/local/bin/python
# -*- coding: utf-8 -*-
"""
本扩展用于存放分析公共函数
@author tonyfeng
@date  2014.1013 
"""
import os
import sys
import re
import time
import shutil
import string
import hashlib
import urllib
import httplib 
import json
from urlparse import urlparse
from decimal import Decimal

def read_file( file_path ):
    """
    一次性把文件内容给读取进来
    @param string file_path 文件路径
    @return list s 返回文件的内容
    """
    try:
        file = open(file_path,'r')
        s = file.read()
        file.close()
        return s
    except:
        return []


def mv_file( old_file,new_dir ):    
    """
    移文件到新的目录下
    @param string old_file 旧文件路径
    @param string new_dir 新文件夹路径
    @return void
    """
    file_name = os.path.basename(old_file)
    #判断新目录是否存在，不存在的话创建目录
    if not os.path.isdir(new_dir):
        os.makedirs(new_dir,0766)
    try:
        new_file = os.path.join( new_dir, file_name)
        if os.path.isfile(old_file):
            shutil.copyfile( old_file, new_file)
            os.remove(old_file)
    except:
        print "fuck you,I cannot make dir"
        pass    

def get_current_ip(ifname):
    '''
    获取当前服务器的IP地址
    @param ifname =eth0|eth1
    '''
    import socket,fcntl,struct
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    inet = fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))
    ipaddr = socket.inet_ntoa(inet[20:24])
    
    return ipaddr

def phptrim(zstr):
    '''
    去除字符串两边空隔
    @param zstr
    '''
    ystr = zstr.lstrip()
    ystr = ystr.rstrip()
    ystr = ystr.strip()
    return ystr


def uniq_list(arrays):
    '''
    返回重复值及它的位置
    @param arrays LIST
    '''
    dd = defaultdict(list)
    for k,va in [(v,i) for i,v in enumerate(arrays)]:
        dd[k].append(va)
    return dd    

def write_file(file_name,text,type = 'w'):
    '''
    写入文件
    @param string file_name 文件名
    @param string text 文本
    '''
    try:
        fp = open(file_name,type)
        fp.write(text  + '\n')
    finally:
        fp.close()

def format_fee(fee):
    '''
    格式化金额，保留两位小数点
    '''
    fee = '{:.6f}'.format(Decimal(str(fee)))
    fee = float(fee)
    return fee

def mkdirs(path): 
    '''
    创建多层目录
    @param string path
    @return boolean
    '''
    path = path.strip()
    path = path.rstrip("\\")
    is_exists = os.path.exists(path)
    
    if not is_exists:  #判断结果
        os.makedirs(path)
        return True
    else:
        return False

def md5(str):
    '''
    字符串加密
    @param string str
    @return string
    '''
    h = hashlib.md5()
    h.update(str)
    return h.hexdigest()

def post_body_submit(requrl,body,header_data):
    '''
    POST请求
	@param string requrl 
	@param dict body
	@param dict header_data
    '''
    parts = urlparse(requrl)
    host = parts.netloc
    encode_body = json.dumps(body)
    conn = httplib.HTTPConnection(host)
    conn.request(method="POST",url = requrl,body = encode_body,headers = header_data) 
    response = conn.getresponse()
    res = response.read()
    res = json.loads(res)

    return res

def restor_ios_idfa(idfa):

    if len(idfa) != 32:
        return idfa

    idfa = idfa[0:8] + '-' + idfa[8:12] + '-' + idfa[12:16] + '-' + idfa[16:20] + '-' + idfa[20:32]

    return idfa

	




