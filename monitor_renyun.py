#!/usr/local/bin/python
# -*- coding: utf-8 -*-
'''
首次打开应用数据上报到热云
@author tonyfeng
@date  2016.11.10
'''
import sys
import os
import time
import urllib2
import urllib
import datetime
import json
import string
import re
import subprocess
import include.common_def as common
import include.model_def as model
import include.config as config


'''
exec script
'''

#定义监控日志时间（钞）
monitor_seconds = 300
sleep_seconds = 10

if __name__ == '__main__' :
    now_time = time.localtime(time.time())
    print time.strftime("%Y-%m-%d %H:%M:%S", now_time) + '-monitor_renyun-start'
    #file_path = "/proj/sh/reyun/renyun/"
    file_path = "/proj/data/tonyfile/renyun/"

    while True:

        print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))

        date_str = time.strftime("%Y%m%d", time.localtime(time.time()))
        action = ['startup','install','payment','register','loggedin']
        
        try:
            for ac in action:
        
                cmd = "cat "+ file_path +"logs/reyun-success-"+ date_str +".txt | grep "+ ac +" | tail -1"
                code = subprocess.Popen(cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                message = cmd + "^"
                msg = code.communicate()[0]
                matchObj = re.search( r'value=((.?)+)[,]', msg, re.M|re.I)
                if matchObj:
                    time_str = matchObj.group(1)
                else:
                    print 1
                    #time.sleep(sleep_seconds)
                    continue
                
                #计算时差
                nowtime = datetime.datetime.now()
                date1 = time.strptime(str(time_str + '.500'),"%Y-%m-%d %H:%M:%S.%f")
                last_logs_time = datetime.datetime(date1[0],date1[1],date1[2],date1[3],date1[4],date1[5])
                s = (nowtime - last_logs_time).seconds
                message += str(s) + "|" + str(time_str) +"^"

                #超出规定时间内没有日志，开启分析脚本，否则继续跳出
                if s < monitor_seconds: 
                    print 2
                    #time.sleep(sleep_seconds)
                    continue

                #KILL掉当前执行python
                cmd = "ps -ax | grep "+ ac +"_data_submit | awk -F ' ' '{print $1}'"
                code = subprocess.Popen(cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                message += cmd + "^"
                
                pid_lists = code.communicate()[0].split("\n")
                pid_str = " ".join(pid_lists)
                cmd = "kill -9 " + pid_str
                subprocess.Popen(cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                message += cmd + "^"
                
                #启动
                script_name = file_path + ac +"_data_submit.py " + date_str
                cmd = "nohup /usr/bin/python "+ script_name +" >/dev/null 2>&1 & "
                code = subprocess.Popen(cmd,shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
                message += cmd + "^"

                #写入LOGS
                message =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())) + "|monitor-ok|" + message
                common.write_file(file_path + '/logs/renyun-monitor-'+date_str+'.txt', message, 'a')
                print message

                time.sleep(sleep_seconds)
        
        except Exception,ex:
            print ex.message
            message =  time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time())) + "|monitor-try|" + str(ex.message)
            common.write_file(file_path + '/logs/renyun-monitor-'+date_str+'.txt', message, 'a')
            print message
            time.sleep(sleep_seconds)
                
        print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))
    
    print time.strftime("%Y-%m-%d %H:%M:%S", now_time) + '-monitor_renyun-end'

