#import json
import sqlite3

# SQLite DB Name
DB_Name =  "wban_corby.db"

#===============================================================
# Database Manager Class

class DatabaseManager():
	def __init__(self):
		self.conn = sqlite3.connect(DB_Name)
		self.conn.execute('pragma foreign_keys = on')
		self.conn.commit()
		self.cur = self.conn.cursor()
		
	def add_del_update_db_record(self, sql_query, args=()):
		self.cur.execute(sql_query, args)
		self.conn.commit()
		return

	def __del__(self):
		self.cur.close()
		self.conn.close()

#===============================================================
# Functions to push Sensor Data into Database

# Function to save Temperature to DB Table
def acc_Data_Handler(item):
	#Parse Data
    Ax = item
    Ay = "4.00"
    Az = "5.00"
    #if item[0:2] == "Ax":
     #   Ax = item[3:0]
    #elif item[0:2] == "Ay":
     #   Ay = item[3:0]
    #elif item[0:2] == "Az":
     #   Az = item[3:0]
	
	#Push into DB Table
	dbObj = DatabaseManager()
	dbObj.add_del_update_db_record("insert into wban_corby (Ax, Ay, Az) values (?,?,?)",[Ax, Ay, Az])
	del dbObj
	print "Inserted Accelerometer Data into Database."
	print ""

# Function to save Humidity to DB Table


#===============================================================
# Master Function to Select DB Funtion based on MQTT Topic

def sensor_Data_Handler(Data):
        #if Topic == "wban/sensor/accelerometer":
         #   item = str(Data.decode("utf-8"))
            acc_Data_Handler(Data)
	
sensor_Data_Handler("4.00")
#===============================================================