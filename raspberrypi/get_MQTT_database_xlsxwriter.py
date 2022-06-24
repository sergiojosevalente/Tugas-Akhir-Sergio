import paho.mqtt.client as mqtt
import pandas as pd
import xlsxwriter
#from get_MQTT_savedatabase_sql import sensor_Data_Handler


MQTT_ADDRESS = '192.168.43.248'
MQTT_USER = 'sergiocorbymqtt'
MQTT_PASSWORD = 'SergioCorby'
MQTT_TOPIC = 'wban/sensor/accelerometer'

Ax = []
Ay = []
Az = []

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    global sensor 
    """The callback for when a PUBLISH message is received from the server."""
    #print(msg.payload)
    item = str(msg.payload.decode("utf-8"))
    #print(item)
    if item[0:2] == "Ax":
        sensor = 1
        savexlsx()
    elif item[0:2] == "Ay":
        sensor = 2
        savexlsx()
    elif item[0:2] == "Az":
        sensor = 3
        savexlsx()
    else:
        Sensor = 0
        savexlsx()
        
def savexlsx():
    global Ax, Ay, Az, sensor, worksheet, workbook
    
    print(sensor)
    
    if sensor == 1:
        workbook = xlsxwriter.Workbook('ESPKanan.xlsx')
        worksheet = workbook.add_worksheet()
        worksheet.write(0, 0, "Ax")
        worksheet.write(0, 1, "Ay")
        worksheet.write(0, 2, "Az")
        col = 0
    elif sensor ==2:
        col = 1
    elif sensor ==3:
        col = 2
    row = 1
    for x in range(20):
        worksheet.write(row, col, x)
        row +=1
        
        #elif len(AZ)==4:
        #print(AX)
        
        #data = {
        #    "Ax": AX, "Ay":AY, "Az": AZ
        #}
        #df = pd.DataFrame(data)
        #df.to_csv('done.csv',index=False)
        #print("Thanks God")
    

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



