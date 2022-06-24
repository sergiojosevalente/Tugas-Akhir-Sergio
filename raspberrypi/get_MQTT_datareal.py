import paho.mqtt.client as mqtt
#from get_MQTT_savedatabase_sql import sensor_Data_Handler


MQTT_ADDRESS = '192.168.43.248'
MQTT_USER = 'sergiocorbymqtt'
MQTT_PASSWORD = 'SergioCorby'
MQTT_TOPIC = 'wban/sensor/accelerometer'

AX,AY,AZ = [],[],[]

import sqlite3

# SQLite DB Name
#DB_Name =  "wban_corby.db"

#===============================================================
# Database Manager Class

#class DatabaseManager():
#	def __init__(self):
#		self.conn = sqlite3.connect(DB_Name)
#		self.conn.execute('pragma foreign_keys = on')
#		self.conn.commit()
#		self.cur = self.conn.cursor()
		
#	def add_del_update_db_record(self, sql_query, args=()):
#		self.cur.execute(sql_query, args)
#		self.conn.commit()
#		return

#	def __del__(self):
#		self.cur.close()
#		self.conn.close()

#===============================================================
# Functions to push Sensor Data into Database

# Function to save Temperature to DB Table
#def acc_Data_Handler(item):
	#Parse Data
 #   if item[0:2] == "Ax":
 #       Ax = item[3:0]
 #   elif item[0:2] == "Ay":
 #       Ay = item[3:0]
 #   elif item[0:2] == "Az":
 #       Az = item[3:0]
	
	#Push into DB Table
	#dbObj = DatabaseManager()
	#dbObj.add_del_update_db_record("insert into wban_corby (Ax, Ay, Az) values (?,?,?)",[Ax, Ay, Az])
	#del dbObj
#	print("Inserted Accelerometer Data into Database.")
#	print("")

# Function to save Humidity to DB Table


#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

#def sensor_Data_Handler(Topic, Data):
#        if Topic == "wban/sensor/accelerometer":
#            item = str(Data.decode("utf-8"))
#            acc_Data_Handler(item)
	

#===============================================================

def on_connect(client, userdata, flags, rc):
    """ The callback for when the client receives a CONNACK response from the server."""
    print('Connected with result code ' + str(rc))
    client.subscribe(MQTT_TOPIC)


def on_message(client, userdata, msg):
    """The callback for when a PUBLISH message is received from the server."""
    print(msg.payload)
    #sensor_Data_Handler(mgs.topic,msg.payload)
    

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

