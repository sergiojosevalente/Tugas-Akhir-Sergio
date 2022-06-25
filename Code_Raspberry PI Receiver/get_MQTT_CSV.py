import paho.mqtt.client as mqtt
import csv
import pandas as pd

MQTT_ADDRESS = '192.168.43.248'
MQTT_USER = 'sergiocorbymqtt'
MQTT_PASSWORD = 'SergioCorby'
MQTT_TOPIC = 'wban/sensor/#'

header = ['Ax1','Ay1','Az1','Rs1']
header = ['Ax2','Ay2','Az2','Rs2']
header = ['Ax3','Ay3','Az3','Rs3']

AX1,AY1,AZ1,RS1 = [],[],[],[]
AX2,AY2,AZ2,RS2 = [],[],[],[]
AX3,AY3,AZ3,RS3 = [],[],[],[]

X1,Y1,Z1,R1 = [], [], [], []
X2,Y2,Z2,R2 = [], [], [], []
X3,Y3,Z3,R3 = [], [], [], []

data_size = 10


def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    #print(msg.payload)
    item = str(msg.payload.decode("utf-8"))
    
#ESP1
    
#===========================================================#
    if msg.topic == 'wban/sensor/esp1':
    #Data accesp1
        if  len(RS1)<data_size - 1:
            if item[0:6] == "Axesp1":
                Ax1 = item[7:]
                AX1.append(Ax1)
            elif item[0:6] == "Ayesp1":
                Ay1=item[7:]
                AY1.append(Ay1)
            elif item[0:6] == "Azesp1":
                Az1=item[7:]
                AZ1.append(Az1)
            elif item[0:8] == "Rssiesp1":
                Rs1=item[9:]
                RS1.append(Rs1)
            
            
        #Data accbagi3
        elif len(AX1)==data_size -1 or len(AY1)==data_size -1 or len(AZ1)==data_size -1 or len(RS1)==data_size -1:
            
            if item[0:6] == "Axesp1":
                Ax1 = item[7:]
                AX1.append(Ax1)
            elif item[0:6] == "Ayesp1":
                Ay1=item[7:]
                AY1.append(Ay1)
            elif item[0:6] == "Azesp1":
                Az1=item[7:]
                AZ1.append(Az1)
            elif item[0:8] == "Rssiesp1":
                Rs1=item[9:]
                RS1.append(Rs1)
                accbagitiga1()

    
#ESP2
    
#===========================================================#
    elif msg.topic == 'wban/sensor/esp2':
    #Data accesp2
        if  len(RS2)<data_size - 1:
            if item[0:6] == "Axesp2":
                Ax2 = item[7:]
                AX2.append(Ax2)
            elif item[0:6] == "Ayesp2":
                Ay2=item[7:]
                AY2.append(Ay2)
            elif item[0:6] == "Azesp2":
                Az2=item[7:]
                AZ2.append(Az2)
            elif item[0:8] == "Rssiesp2":
                Rs2=item[9:]
                RS2.append(Rs2)
            
            
        #Data accbagi3
        elif len(AX2)==data_size -1 or len(AY2)==data_size -1 or len(AZ2)==data_size -1 or len(RS2)==data_size -1:
            
            if item[0:6] == "Axesp2":
                Ax2 = item[7:]
                AX2.append(Ax2)
            elif item[0:6] == "Ayesp2":
                Ay2=item[7:]
                AY2.append(Ay2)
            elif item[0:6] == "Azesp2":
                Az2=item[7:]
                AZ2.append(Az2)
            elif item[0:8] == "Rssiesp2":
                Rs2=item[9:]
                RS2.append(Rs2)
                accbagitiga2()

    
# ESP3
#===========================================================#
    
    
    elif msg.topic == 'wban/sensor/esp3':
    #Data accesp3
        if  len(RS3)<data_size - 1:
            if item[0:6] == "Axesp3":
                Ax3 = item[7:]
                AX3.append(Ax3)
            elif item[0:6] == "Ayesp3":
                Ay3=item[7:]
                AY3.append(Ay3)
            elif item[0:6] == "Azesp3":
                Az3=item[7:]
                AZ3.append(Az3)
            elif item[0:8] == "Rssiesp3":
                Rs3=item[9:]
                RS3.append(Rs3)
            
            
        #Data accbagi3
        elif len(AX3)==data_size -1 or len(AY3)==data_size -1 or len(AZ3)==data_size -1 or len(RS3)==data_size -1:
            
            if item[0:6] == "Axesp3":
                Ax3 = item[7:]
                AX3.append(Ax3)
            elif item[0:6] == "Ayesp3":
                Ay3=item[7:]
                AY3.append(Ay3)
            elif item[0:6] == "Azesp3":
                Az3=item[7:]
                AZ3.append(Az3)
            elif item[0:8] == "Rssiesp3":
                Rs3=item[9:]
                RS3.append(Rs3)
                accbagitiga3()

#=====================================================================#

def accbagitiga1():
    dataacc1 = {"Ax1": AX1, "Ay1":AY1, "Az1": AZ1, "Rs1":RS1}
    df1acc= pd.DataFrame(dataacc1)
    df1acc.to_csv('dataacc1.csv',index=True)


    for i in range(0,len(RS1)):
        #print(float(AX[i])
        if i == 0 :
            x1 = (float(AX1[i])+float(AX1[i])+float(AX1[i]))/3
            y1 = (float(AY1[i])+float(AY1[i])+float(AY1[i]))/3
            z1 = (float(AZ1[i])+float(AZ1[i])+float(AZ1[i]))/3
        elif i == 1:
            x1 = (float(AX1[i])+float(AX1[i-1])+float(AX1[i]))/3
            y1 = (float(AY1[i])+float(AY1[i-1])+float(AY1[i]))/3
            z1 = (float(AZ1[i])+float(AZ1[i-1])+float(AZ1[i]))/3
        else:
            x1 = (float(AX1[i-2])+float(AX1[i-1])+float(AX1[i]))/3
            y1 = (float(AY1[i-2])+float(AY1[i-1])+float(AY1[i]))/3
            z1 = (float(AZ1[i-2])+float(AZ1[i-1])+float(AZ1[i]))/3
    
        X1.append(x1)
        Y1.append(y1)
        Z1.append(z1)
        
    datacc1bagi3 = {"x1": X1, "y1":Y1, "z1": Z1, "R1":RS1}
    df1accbagi3 = pd.DataFrame(datacc1bagi3)
    df1accbagi3.to_csv('dataacc1bagi3.csv',index=True)
def accbagitiga2():
    dataacc2 = {"Ax2": AX2, "Ay2":AY2, "Az2": AZ2, "Rs2":RS2}
    df2acc= pd.DataFrame(dataacc2)
    df2acc.to_csv('dataacc2.csv',index=True)


    for i in range(0,len(RS2)):
        if i == 0 :
            x2 = (float(AX2[i])+float(AX2[i])+float(AX2[i]))/3
            y2 = (float(AY2[i])+float(AY2[i])+float(AY2[i]))/3
            z2 = (float(AZ2[i])+float(AZ2[i])+float(AZ2[i]))/3
        elif i == 1:
            x2 = (float(AX2[i])+float(AX2[i-1])+float(AX2[i]))/3
            y2 = (float(AY2[i])+float(AY2[i-1])+float(AY2[i]))/3
            z2 = (float(AZ2[i])+float(AZ2[i-1])+float(AZ2[i]))/3
        else:
            x2 = (float(AX2[i-2])+float(AX2[i-1])+float(AX2[i]))/3
            y2 = (float(AY2[i-2])+float(AY2[i-1])+float(AY2[i]))/3
            z2 = (float(AZ2[i-2])+float(AZ2[i-1])+float(AZ2[i]))/3
    
        X2.append(x2)
        Y2.append(y2)
        Z2.append(z2)
        
    datacc2bagi3 = {"x2": X2, "y2":Y2, "z2": Z2, "R2":RS2}
    df2accbagi3 = pd.DataFrame(datacc2bagi3)
    df2accbagi3.to_csv('dataacc2bagi3.csv',index=True)

def accbagitiga3():
    dataacc3 = {"Ax3": AX3, "Ay3":AY3, "Az3": AZ3, "Rs3":RS3}
    df3acc= pd.DataFrame(dataacc3)
    df3acc.to_csv('dataacc3.csv',index=True)


    for i in range(0,len(RS3)):
        if i == 0 :
            x3 = (float(AX3[i])+float(AX3[i])+float(AX3[i]))/3
            y3 = (float(AY3[i])+float(AY3[i])+float(AY3[i]))/3
            z3 = (float(AZ3[i])+float(AZ3[i])+float(AZ3[i]))/3
        elif i == 1:
            x3 = (float(AX3[i])+float(AX3[i-1])+float(AX3[i]))/3
            y3 = (float(AY3[i])+float(AY3[i-1])+float(AY3[i]))/3
            z3 = (float(AZ3[i])+float(AZ3[i-1])+float(AZ3[i]))/3
        else:
            x3 = (float(AX3[i-2])+float(AX3[i-1])+float(AX3[i]))/3
            y3 = (float(AY3[i-2])+float(AY3[i-1])+float(AY3[i]))/3
            z3 = (float(AZ3[i-2])+float(AZ3[i-1])+float(AZ3[i]))/3
    
        X3.append(x3)
        Y3.append(y3)
        Z3.append(z3)
        
    datacc3bagi3 = {"x3": X3, "y3":Y3, "z3": Z3, "R3":RS3}
    df3accbagi3 = pd.DataFrame(datacc3bagi3)
    df3accbagi3.to_csv('dataacc3bagi3.csv',index=True)
    
    
    
    print("Data saved")

def main():
    mqtt_client = mqtt.Client()
    mqtt_client.username_pw_set(MQTT_USER, MQTT_PASSWORD)
    mqtt_client.on_connect = on_connect
    mqtt_client.on_message = on_message

    mqtt_client.connect(MQTT_ADDRESS, 1883)
    mqtt_client.loop_forever()


if __name__ == '__main__':
    print('MQTT to InfluxDB bridge')
    main()


