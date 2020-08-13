import paho.mqtt.client as mqtt
import sched, time
from queue import Queue 
from datetime import datetime
import bitstruct
import struct
from subprocess import check_output
import psutil
import threading
import sys
import win32evtlogutil
import win32evtlog
from datetime import datetime
import serial
import logging
from tb_device_mqtt import TBDeviceMqttClient, TBPublishInfo
import json

client_name="test sipora"

#serialCPU = serial.Serial('COM5', 115200)

###############################################################################################
# Class MQTT Start
###############################################################################################
    
class client:
    __BPR = 0
    __ACLO = 1
    brokerParameter = {"broker_address":"127.0.0.1", "port":1883, "user name":"", "password":""}
    def __init__(self):
        pass
        #self.initialize()

    def initialize(self):
        self.__initializeDataFormat()
        try:
            user =mqtt.Client("PC1")
        except:
            print("failed to create client instance...")
            win32evtlogutil.ReportEvent(
            "MQQT_CLIENT", 32010, eventCategory=32010,
            eventType=win32evtlog.EVENTLOG_ERROR_TYPE, strings=["failed to create client instance..."],
            data=b"failed to create client instance...")
        
        try:
            user.username_pw_set(self.brokerParameter["user name"], self.brokerParameter["password"])
        except:
            print("failed to set user name or password...")

        self.user = user
    
    def setBrokerParameter(self, broker_address, port=1883, userName="", password=""):
        try:
            self.brokerParameter["broker_address"] = broker_address
            self.brokerParameter["user name"] = userName
            self.brokerParameter["password"] = password
            self.brokerParameter["port"] = port
        except:
            print("failed to set broker parameter...")

    def __initializeDataFormat(self):
        try:
            __bpr   = bitstruct.compile('u10u2s32u28s16')
            __aclo  = bitstruct.compile('u10u2s32s20s20s20')
            self.__bpr = __bpr
            self.__aclo = __aclo
        except:
            print("failed to initialize data format...")
            win32evtlogutil.ReportEvent(
            "MQQT_CLIENT", 32011, eventCategory=32011,
            eventType=win32evtlog.EVENTLOG_ERROR_TYPE, strings=["failed to initialize data format..."],
            data=b"failed to initialize data format...")
    
    def __runLocalBrokerServiceChecker(self):
        schedulerStop = False
        self.schedulerStop = schedulerStop
        schedule.every(5).minutes.do(self.__checkLocalBrokerService)
        while True:
            schedule.run_pending()
            if (self.schedulerStop == True):
                break  

    def __checkLocalBrokerService(self):
        service = self.__getService("mosquitto")
        if service and service['status'] == 'running':
            print("service is running")
        else:
            check_output("net start mosquitto", shell=True).decode()
            print("service is not running")

    def connect(self):
        try:
            print("connect")
            self.user.connect(self.brokerParameter["broker_address"], port=self.brokerParameter["port"])
            win32evtlogutil.ReportEvent(
            "MQQT_CLIENT", 32012, eventCategory=32012,
            eventType=win32evtlog.EVENTLOG_SUCCESS, strings=["Connect process successfull..."],
            data=b"Connect successfully...")
        except:
            print("failed to connect...")
            win32evtlogutil.ReportEvent(
            "MQQT_CLIENT", 32013, eventCategory=32013,
            eventType=win32evtlog.EVENTLOG_ERROR_TYPE, strings=["failed to connect..."],
            data=b"failed to connect...")
    
    def disconnect(self):
        try:
            self.user.disconnect()
            self.schedulerStop = True
            win32evtlogutil.ReportEvent(
            "MQQT_CLIENT", 32014, eventCategory=32014,
            eventType=win32evtlog.EVENTLOG_SUCCESS, strings=["Disconnect process successfull..."],
            data=b"Disconnect successfully...")
        except:
            print("failed to disconnect...")
            win32evtlogutil.ReportEvent(
            "MQQT_CLIENT", 32015, eventCategory=32015,
            eventType=win32evtlog.EVENTLOG_ERROR_TYPE, strings=["failed to disconnect..."],
            data=b"failed to disconnect...")

    def __timestampFormatter(self, dt, epoch=datetime(1970,1,1)):
        try:
            td = dt - epoch
            return (td.microseconds + (td.seconds + td.days * 86400) * 10**6) / 10**6
        except:
            print("failed to format time stamp...")

    def __makePayload(self, data, sensorType):
        id = 3
        payload=""

        try:
            timeNow = self.__timestampFormatter(datetime.utcnow())
        except:
            print("failed to get time...")

        try:
            if(sensorType == 0): 
                bprValue = data.split(",")
                payload = self.__bpr.pack(id, 0, int(timeNow),float(bprValue[0])*100,float(bprValue[1])*100)
            elif(sensorType == 1):
                acclAxisValue = data.split(",")
                payload = self.__aclo.pack(id, 1, int(timeNow),float(acclAxisValue[0])*100,float(acclAxisValue[1])*100,float(acclAxisValue[2])*100)
            elif(sensorType == 2):
                payload = struct.pack("i", id)+struct.pack("i", timeNow)+data.encode("utf-8")

        except:
            print("failed  create payload...")
            win32evtlogutil.ReportEvent(
            "MQQT_CLIENT", 32016, eventCategory=32016,
            eventType=win32evtlog.EVENTLOG_ERROR_TYPE, strings=["Failed to create payload..."],
            data=b"Failed to create payload...")
        
        return payload

    def ____todatetimes(self,ts, frm='%Y-%m-%d %H:%M:%S'):
	    return datetime.utcfromtimestamp(ts).strftime(frm)

    def publishData(self, data, topic, sensorType, qosValue=1):
        try:
            self.initialize()
            self.connect()
            payload = self.__makePayload(data, sensorType)
            self.user.publish(topic, payload, qos=qosValue)
            self.user.loop()
            self.disconnect()
        except:
            print("failed to publish...")
            win32evtlogutil.ReportEvent(
            "MQQT_CLIENT", 32017, eventCategory=32017,
            eventType=win32evtlog.EVENTLOG_ERROR_TYPE, strings=["Failed to publish..."],
            data=b"Failed to publish...")

    def __getService(self, name):
        service = None
        try:
            service = psutil.win_service_get(name)
            service = service.as_dict()
        except Exception as ex:
            print(str(ex))
        return service

###################################################################################################
# Class MQTT End
###################################################################################################

###################################################################################################
# Function Start
###################################################################################################

# Function to save data pressure, input data float from Pressure value

def writelogdatapower(data):
    now = datetime.now() # current date and time
    date_time = now.strftime("%d-%m-%Y")
    date_time_write = now.strftime("%d,%m,%Y,%H,%M,%S,")
    namelogfile = "LogDataSiporaPower_" + date_time + ".txt"
    filelog = open(namelogfile,"a")
    datasave = date_time_write + str(data) + '\n'
    filelog.write(datasave)
    filelog.close()

def parsingACC(payload):
    datax = str(payload)
    data = datax.split(":")
    data1 = data[1].split(",")
    return data1

    
###################################################################################################
# Function End
###################################################################################################
  
###################################################################################################
# Function Thread Start
###################################################################################################

#creating message queue
powerDataQueue = Queue()

# creating a lock 
lock = threading.Lock() 

def prosesPOWER():
    print('POWER\n')
    serialACC = serial.Serial('COM10', 9600)
    while True:
        dataInput = serialACC.readline()[:-2]
        data=dataInput.decode('utf-8')
        print(data)
        if data:
            lock.acquire()
            powerDataQueue.put(data)
            lock.release()
            writelogdatapower(data)

def prosesIOT():

    print('IoT\n')
    clientthingsboard = TBDeviceMqttClient("202.46.7.33", "TEWSSiporaPower")
    clientthingsboard.connect()
    while True:
        if (powerDataQueue.empty() == False):

            #get data queue
            lock.acquire()
            powerdata = powerDataQueue.get()
            lock.release()

            #send to RDS
            #clientRDS = client()
            #payload = str(xdata) + ',' + str(ydata) + ',' + str(zdata)
            #clientRDS.setBrokerParameter("202.46.3.41",1883, userName="tews-siberut", password="tews_2019")
            #clientRDS.publishData(payload, "node/sensor/siberut/accelerometer", sensorType=1, qosValue=1)

            #send to thingsboard
            telemetry_with_ts = {"ts": int(round(time.time() * 1000)), "values": {"volt1": float(powerdata), "volt2": float(powerdata)}}
            clientthingsboard.send_telemetry(telemetry_with_ts)                         

    clientthingsboard.disconnect()


###################################################################################################
# Function Thread End
###################################################################################################



def main_task(): 

    start = time.perf_counter()
  
    # creating threads 
    taskPOWER = threading.Thread(target=prosesPOWER) 
    taskIOT = threading.Thread(target=prosesIOT)
  
    # start threads 
    taskPOWER.start() 
    taskIOT.start()
  
    # wait until threads finish their job 
    taskPOWER.join() 
    taskIOT.join()
  
if __name__ == "__main__":     
    main_task() 
