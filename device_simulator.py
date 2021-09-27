import boto3
import time
import json
import argparse
import pandas as pd
import numpy as np
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# 配置默认设备状态为开启
global control
control = "on"


# 当IoT Shadow状态被更新后，执行该函数，关闭设备
def customCallback(client, userdata, message):
    jsonState = json.loads(message.payload)
    state = jsonState ['state'] 
    desired = state ['desired']
    switch = desired['switch']


    if switch == "off":
        print("  ")
        print("  ")
        print("*******************************************************************")
        print("Machine Learning Predicted Failure...")
        print("Initiating turbine shutdown ....\n")
        print("Turbine shutdown completed...")
        print("*******************************************************************")
        print("  ")
        print("  ")

        global control
        control = "off"

    if switch == "on":
        print("Activating turbine...\n....\n")
        time.sleep(1)
        print("Starting to sent telemetry data to AWS IoT...\n"  )       
        control = "on"

# 配置IoT Core Endpoint和证书路径
host = "a1hk0pcc0as07l.ats.iot.cn-northwest-1.amazonaws.com.cn"
rootCAPath = "cert/root-CA.crt" 
certificatePath = "cert/windturbine.cert.pem"
privateKeyPath = "cert/windturbine.private.key" 


print(" ")
print("*************************************")
print("Activating turbines...\n")
print(" ")
time.sleep(2)
print("Starting to sent telemetry to AWS IoT...")
print("*************************************")
print(" ")

time.sleep(2)

# 初始化IoT Client
myAWSIoTMQTTClient = None

myAWSIoTMQTTClient = AWSIoTMQTTClient("windturbine/xgboost")
myAWSIoTMQTTClient.configureEndpoint(host, 8883)
myAWSIoTMQTTClient.configureCredentials(rootCAPath, privateKeyPath, certificatePath)

# 连接到IoT Core
myAWSIoTMQTTClient.connect()

# 订阅设备影子并且当shadow被更新时调用函数customCallback
myAWSIoTMQTTClient.subscribe("$aws/things/windturbine/shadow/update", 1, customCallback) 

# 设置读取csv文件的参数 
parser = argparse.ArgumentParser()
parser.add_argument('datafile', help="read lines from csv file")
args = parser.parse_args()

# 从csv文件中读取数据
data = pd.read_csv(args.datafile)

# 发送数据到IoT Core
while len(data) >= 1:

    time.sleep (3) 

    # use variable control to check if it is set to "on", 
    # then only proceed with sending the telemetry, in case it is "off" we will not. 
    # this variable is switched to "off" in the customCallback function, 
    # when device recives a message to stop the device.
   
    if control == "on":
        
        #reading the first line from the csv file
        line = data.iloc[0, :]
        
        # remove the first line from the csv file 
        data.drop(data.index[0], inplace=True)
        
        
        print("----------------------------------------------------------------------------------------------------------------")
        

        jpayload = {}
 
        # prepare json payload
       
        jpayload ['wind_speed'] = int(line['wind_speed'])
        jpayload ['RPM_blade'] = int(line['RPM_blade'])
        jpayload ['oil_temperature'] = int(line['oil_temperature'])
        jpayload ['oil_level'] = int(line['oil_level'])
        jpayload ['temperature'] = int(line['temperature'])
        jpayload ['humidity'] = int(line['humidity'])
        jpayload ['vibrations_frequency'] = int(line['vibrations_frequency'])
        jpayload ['pressure'] = int(line['pressure'])
        jpayload ['wind_direction'] = int(line['wind_direction'])

        json_data = json.dumps(jpayload)    

        print(json_data)
        print("\n" )

        # publish to the topic
        myAWSIoTMQTTClient.publish("windturbine/xgboost", json_data, 1)
        

