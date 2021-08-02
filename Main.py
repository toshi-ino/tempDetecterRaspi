# -*- coding: utf-8 -*-

#センサーや検知方法の設定
from detectMethod import EnvSensorClass
from time import sleep
import math
from decimal import Decimal
import random

INTERVAL = 60 
SENSOR_PIN_TEMP_HUMI = 4
RETRY_TIME = 2
MAX_RETRY = 10
N = 1

import RPi.GPIO as GPIO
GPIO.setwarnings(False) # GPIO.cleanup()をしなかった時のメッセージを非表示にする
GPIO.setmode(GPIO.BCM) # ピンをGPIOの番号で指定


#通信方法の設定
from awscrt import io, mqtt, auth, http
from awsiot import mqtt_connection_builder
import time as t
import json
from datetime import datetime

import settings  #環境変数
ENDPOINT = settings.ENDPOINT_set
CLIENT_ID = settings.CLIENT_ID_set
PATH_TO_CERT = settings.PATH_TO_CERT_set
PATH_TO_KEY = settings.PATH_TO_KEY_set
PATH_TO_ROOT = settings.PATH_TO_ROOT_set
TOPIC = "data/" + CLIENT_ID


event_loop_group = io.EventLoopGroup(1)
host_resolver = io.DefaultHostResolver(event_loop_group)
client_bootstrap = io.ClientBootstrap(event_loop_group, host_resolver)
mqtt_connection = mqtt_connection_builder.mtls_from_path(
            endpoint=ENDPOINT,
            cert_filepath=PATH_TO_CERT,
            pri_key_filepath=PATH_TO_KEY,
            client_bootstrap=client_bootstrap,
            ca_filepath=PATH_TO_ROOT,
            client_id=CLIENT_ID,
            clean_session=False,
            keep_alive_secs=6
            )


#AWSとの接続
print("Connecting to {} with client ID '{}'...".format(
        ENDPOINT, CLIENT_ID))
# Make the connect() callMAX_RETRY
connect_future = mqtt_connection.connect()
# Future.result() waits until a result is available
connect_future.result()
print("Connected!")
# Publish message to server desired number of times.
print('Begin Publish')



#main
try:
    if __name__ == "__main__":
        env = EnvSensorClass()
        while True:

            tempRow, humRow = env.GetTemp(SENSOR_PIN_TEMP_HUMI ,RETRY_TIME,MAX_RETRY) # 温湿度を取得
            
            temp = math.floor(tempRow *10 ** N) / (10 **N) 
            hum = math.floor(humRow *10 ** N) / (10 **N) 
            carbonDioxide = random.randint(280,320)
            PF = random.randint(10,40)

            time = datetime.now().strftime('%Y-%m-%dT%H:%M:%S')
            message = {"DEVICE_NAME": CLIENT_ID,"TIMESTAMP": time,"TEMPERATURE" : int(temp), "HUMIDITY" : int(hum),"CO2": int(carbonDioxide), "pF": int(PF)}
            mqtt_connection.publish(topic=TOPIC, payload=json.dumps(message), qos=mqtt.QoS.AT_LEAST_ONCE)
            print("Published: '" + json.dumps(message) + "' to the topic: " + TOPIC)

            sleep(INTERVAL)

except KeyboardInterrupt:
    pass

GPIO.cleanup()
disconnect_future = mqtt_connection.disconnect()
disconnect_future.result()