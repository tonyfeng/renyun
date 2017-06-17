#!/usr/local/bin/python
# -*- coding: utf-8 -*-
'''
充值数据上报到热云
@author tonyfeng
@date  2014.11.19
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
reload(sys)
sys.setdefaultencoding('utf8')

'''
exec script
'''
if __name__ == '__main__' :
    now_time = time.localtime(time.time())
    print time.strftime("%Y-%m-%d %H:%M", now_time) + '-payment_data_submit-start'
    ptype = {"0": "FREE", "1": "weixinpay", "2":"bank", "3": "alipay","4":"shengpay","5":"Applepurchase","6":"yeepay","7": "unionpay","8": "balance"}
    path = os.path.abspath(os.curdir)
    fileName_consumer_error = 'reyun-consumer-error.txt'
    group_id_state = str(sys.argv[1])

    try:
        consumer = KafkaConsumer('order',group_id='reyun-data-' + group_id_state,bootstrap_servers=config.logs_source_hosts)
        for message in consumer:
        
            message = "%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,message.offset, message.key,message.value)
            print message
            #message = '2016-11-22 11:26:37,232 [INFO] root: bi_order ||L1611226N2023616|2|15532|tc184207256|2016-11-22 11:26:37|157|17|0|1.00|1.00|1.00|0|3||0|0|15532|4|||6|1|862005035210265|a505c6fd5506aef8|f0:43:47:1f:f8:eb'
            logs_array = message.split("|")
            if len(logs_array) < 5:
                continue

            fileName_fail = 'reyun-fail-' + time.strftime("%Y%m%d") + '.txt'
            fileName_success = 'reyun-success-' + time.strftime("%Y%m%d") + '.txt'
            fileName_error = 'reyun-error-' + time.strftime("%Y%m%d") + '.txt'

            body = {"context": {}}
            try:

                # 支付完成的用户才是付费用户
                pay_status = logs_array[19]
                if int(pay_status) != 4:
                    continue

                deviceid = idfa = imei = idfv = transactionid = "unknown"
                transactionid = logs_array[2]
                who = logs_array[5]
                game_id = logs_array[7]
                currencyamount = logs_array[10]

                # 代金券
                if logs_array[14] == '100':
                    continue

                #paymenttype = ptype[logs_array[14]]
                paymenttype = logs_array[14]
                currencytype = 'CNY'
                dev_array = logs_array[24].split('&')
                if len(dev_array) == 3:
                    if len(dev_array[1]) == 32:
                        idfa = common.restor_ios_idfa(dev_array[1])
                        body["context"]["deviceid"] = idfa
                        body["context"]["idfa"] = idfa
                        body["context"]["idfv"] = idfv
                    else:
                        body["context"]["deviceid"] = dev_array[0]
                        body["context"]["imei"] = dev_array[0]
                else:
                    if len(logs_array[24]) == 32:
                        idfa = common.restor_ios_idfa(logs_array[24])
                        body["context"]["deviceid"] = idfa
                        body["context"]["idfa"] = idfa
                        body["context"]["idfv"] = idfv
                    elif len(logs_array[24]) < 32 and len(logs_array[24]) != '':
                        body["context"]["deviceid"] = logs_array[24]
                        body["context"]["imei"] = logs_array[24]

                #获取appkey
                mysql_conn, mysql_cur = model.mysql_connect()
                sql = 'SELECT reyun_appkey FROM reyun_config WHERE is_open = 1 and  game_id = "%s"' % (game_id)
                info = model.query_data(mysql_cur,sql)
                if not info:
                    continue
                info = info[0]
                appkey = info['reyun_appkey']

                #传送的内容体
                body["appid"] = appkey
                body["who"] = who
                body["context"]["transactionid"] = transactionid
                body["context"]["paymenttype"] = paymenttype
                body["context"]["currencytype"] = currencytype
                body["context"]["currencyamount"] = currencyamount

                requrl = config.reyun_uri + 'payment'
                header_data = {"Content-Type":"application/json"}
                result = common.post_body_submit(requrl,body,header_data)

                message = "payment-" + message
                if result['status'] != 0:
                    common.write_file(path + '/logs/' + fileName_fail, message, 'a')
                else:
                    common.write_file(path + '/logs/' + fileName_success, message, 'a')

                print result
            except Exception,ex:
                message = "payment-" + message
                common.write_file(path + '/logs/' + fileName_error, message, 'a')

    except Exception,ex:
        message =  time.strftime("%Y-%m-%d") + "|consumer-payment|" + ex.message
        common.write_file(path + '/logs/' + fileName_consumer_error, message, 'a')
        print message
    
    print time.strftime("%Y-%m-%d %H:%M", now_time) + '-payment_data_submit-end'


