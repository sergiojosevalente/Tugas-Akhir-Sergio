import paho.mqtt.client as mqtt
import json
import pymysql
pymysql.install_as_MySQLdb()
import MySQLdb
from datetime import datetime

db= MySQLdb.connect("localhost", "sensor", "sergiocorbymqtt", "SergioCorby")
cursor=db.cursor()

MQTT_ADDRESS = '192.168.43.248'
MQTT_USER = 'sergiocorbymqtt'
MQTT_PASSWORD = 'SergioCorby'
MQTT_TOPIC = 'wban/sensor/#'


def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    print(msg.topic + ' ' + str(msg.payload))
    cursor.execute ("select * from sensordata")
    numrows = int (cursor.rowcount)
    newrow = numrows + 1
    
    now = datetime.now()
    formatted_date = now.strftime('%Y-%m-%d %H:%M:%S')
    
    payload = json.loads(msg.payload.decode('utf-8'))
    print("New row: "+str(newrow))
    accelerometer_x = float(payload["a.accelerometer.x"])
    accelerometer_y = float(payload["a.accelerometer.y"])
    accelerometer_z = float(payload["a.accelerometer.z"])
    print("Ax: "+str(a.accelerometer.x))
    print("Ay: "+str(a.accelerometer.y))
    print("Az: "+str(a.accelerometer.z))
    print("DateTime: "+str(formatted_date))
    if (( accelerometer_x == 20) and (accelerometer_y == 20)) and ((accelerometer_z == 20)):
      cur = db.cursor()
      cur.execute("INSERT INTO wban.sensordata (idx, ax, ay,az timestamp) VALUES ("+str(newrow)+", "+str(accelerometer_x)+", "+str(accelerometer_y)+","+str(accelerometer_z)+", %s)", (formatted_date))
      db.commit()
      print("data received and imported in MySQL")
    else:
      print("data exceeded limits and is NOT imported in MySQL")

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

