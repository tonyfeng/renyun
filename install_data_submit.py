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
import subprocess
import include.common_def as common
import include.model_def as model
import include.config as config
from kafka import KafkaConsumer


'''
exec script
'''
if __name__ == '__main__' :
    now_time = time.localtime(time.time())
    print time.strftime("%Y-%m-%d %H:%M", now_time) + '-install_data_submit-start'
    path = os.path.abspath(os.curdir)
    fileName_consumer_error = 'reyun-consumer-error.txt'
    group_id_state = str(sys.argv[1])

    try:
        consumer = KafkaConsumer('firstOpenActive',group_id='reyun-data-' + group_id_state,bootstrap_servers=config.logs_source_hosts)
        for message in consumer:
        
            message = "%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value)  
            print message  
            #message = '2016-11-10 15:15:14,872 [INFO] bi: bi_active|65|17|0|yjf_yjz_hqg|2016-11-10 15:15:14|192.168.20.124|ANDROID|865586029885631&fa363d2086afac5&50:68:0a:45:0f:41'
            logs_array = message.split("|")
            if len(logs_array) < 5:
                continue
            
            fileName_fail = 'reyun-fail-' + time.strftime("%Y%m%d") + '.txt'
            fileName_success = 'reyun-success-' + time.strftime("%Y%m%d") + '.txt'
            fileName_error = 'reyun-error-' + time.strftime("%Y%m%d") + '.txt'

            body = {"context": {}}
            try:
                idfa = imei = idfv = androidid = "unknown"
                game_id = logs_array[1]
                channelid = logs_array[2]
                ip = logs_array[6]
                deviceid = logs_array[7].lower()
                dev_array = logs_array[8].split('&')
                if len(dev_array) == 3:
                    if len(dev_array[1]) == 32:
                        idfa = common.restor_ios_idfa(dev_array[1])
                        body["context"]["deviceid"] = idfa
                        body["context"]["idfa"] = idfa
                        body["context"]["idfv"] = idfv
                    else:
                        body["context"]["deviceid"] = dev_array[0]
                        body["context"]["imei"] = dev_array[0]
                        body["context"]["androidid"] = androidid
                else:
                    if deviceid == 'ios':
                        idfa = common.restor_ios_idfa(logs_array[8])
                        body["context"]["deviceid"] = idfa
                        body["context"]["idfa"] = idfa
                        body["context"]["idfv"] = idfv
                    else:
                        #if logs_array[8].find('&') != -1:
                        body["context"]["deviceid"] = logs_array[8]
                        body["context"]["imei"] = logs_array[8]
                        body["context"]["androidid"] = androidid
                    
                #获取appkey
                mysql_conn,mysql_cur = model.mysql_connect()
                sql = 'SELECT reyun_appkey FROM reyun_config WHERE is_open = 1 and game_id = "%s"' % (game_id)
                info = model.query_data(mysql_cur,sql)
                if not info:
                    continue
                info = info[0]
                appkey = info['reyun_appkey']
                
                #传送的内容体
                body["appid"] = appkey
                body["context"]["channelid"] = channelid
                body["context"]["ip"] = ip
                #print body
                
                requrl = config.reyun_uri + 'install'
                header_data = {"Content-Type":"application/json"}
                result = common.post_body_submit(requrl,body,header_data)

                message = "install-" + message
                if result['status'] != 0:
                    common.write_file(path + '/logs/' + fileName_fail, message, 'a')
                else:
                    common.write_file(path + '/logs/' + fileName_success, message, 'a')
                
                print result+'---'
            except Exception,ex:
                message = "install-" + message
                common.write_file(path + '/logs/' + fileName_error, message, 'a')

    except Exception,ex:
        message =  time.strftime("%Y-%m-%d") + "|consumer-install|" + ex.message
        common.write_file(path + '/logs/' + fileName_consumer_error, message, 'a')
        print message
    
    print time.strftime("%Y-%m-%d %H:%M", now_time) + '-install_data_submit-end'

