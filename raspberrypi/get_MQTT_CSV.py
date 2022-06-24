import paho.mqtt.client as mqtt
import csv
import pandas as pd
#from get_MQTT_savedatabase_sql import sensor_Data_Handler


MQTT_ADDRESS = '192.168.43.248'
MQTT_USER = 'sergiocorbymqtt'
MQTT_PASSWORD = 'SergioCorby'
MQTT_TOPIC = 'wban/sensor/accelerometer'

header = ['Ax','Ay','Az']
AX,AY,AZ = [],[],[]
X,Y,Z = [], [], []
data_size = 600


def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    #print(msg.payload)
    item = str(msg.payload.decode("utf-8"))
    #Data acc
    if len(AZ)<data_size - 1:
        if item[0:2] == "Ax":
            Ax = item[3:]
            AX.append(Ax)
        elif item[0:2] == "Ay":
            Ay=item[3:]
            AY.append(Ay)
        elif item[0:2] == "Az":
            Az=item[3:]
            AZ.append(Az)
        #print(item)
            
    #Data accbagi3
    elif len(AZ)==data_size -1 or len(AX)==data_size -1 or len(AY)==data_size -1:
        
        if item[0:2] == "Ax":
            Ax = item[3:]
            AX.append(Ax)
        elif item[0:2] == "Ay":
            Ay=item[3:]
            AY.append(Ay)
        elif item[0:2] == "Az":
            Az=item[3:]
            AZ.append(Az)
            accbagitiga()
    
def accbagitiga():
    data = {
            "Ax": AX, "Ay":AY, "Az": AZ
    }
    df = pd.DataFrame(data)
    df.to_csv('dataacc.csv',index=True)
    
    
    for i in range(0,len(AZ)):
        #print(float(AX[i])
        if i == 0 :
            x = (float(AX[i])+float(AX[i])+float(AX[i]))/3
            y = (float(AY[i])+float(AY[i])+float(AY[i]))/3
            z = (float(AZ[i])+float(AZ[i])+float(AZ[i]))/3
        elif i == 1:
            x = (float(AX[i])+float(AX[i-1])+float(AX[i]))/3
            y = (float(AY[i])+float(AY[i-1])+float(AY[i]))/3
            z = (float(AZ[i])+float(AZ[i-1])+float(AZ[i]))/3
        else:
            x = (float(AX[i-2])+float(AX[i-1])+float(AX[i]))/3
            y = (float(AY[i-2])+float(AY[i-1])+float(AY[i]))/3
            z = (float(AZ[i-2])+float(AZ[i-1])+float(AZ[i]))/3
            
        X.append(x)
        Y.append(y)
        Z.append(z)
        
    data1 = {
        "x": X, "y":Y, "z": Z
    }
    #print(data)
    df = pd.DataFrame(data1)
    df.to_csv('dataaccbagi3.csv',index=True)
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


