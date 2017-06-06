#!/usr/bin/python

import sqlite3

import os
import time
import glob

import Adafruit_DHT

# global variables
speriod     = (15*60)-1
pad         = '/home/hansdej/Sensorlog/rpi_temp_logger-master'
pad         = '/home/hansdej/Sensorlog/AM2302_rpi_weblogger'
dbname      = pad+'/readings.sqlite'
tablename   = "AM2302"
columns     = { 'date': 'DATETIME', 'temperature': 'NUMERIC','moisture': 'NUMERIC'}
# This sqlite database contains a table called "readings"
# with three columns: timestamp, temp and humid.

# A function to prevent injection.
def clean_name(some_var):
    return ''.join(char for char in some_var if char.isalnum())

class SensorLog():
    """
    The object that interfaces to the sqlite3 database.
    """
    def __init__(self, filename,tablename):
        self.filename   = filename
        self.tablename  = tablename

    def make_datatable(self, filename,tablename, tables):
        connection = sqlite3.connect(self.filename)
        cursor     = connection.cursor()

        first   = True
        for col in columns:
            if first:
                # No comma for the first column definition
                theColumns = ""
                first   = False
            else:
                theColumns += ","
            theColumns += "\'%s\' \'%s\'"%(clean_name(col),clean_name(column[col]))
            tablename   = clean_name(tablename)

        cursor.execute("CREATE TABLE %s ( %s )"%(tablename,theColumns))
        connection.commit()
        connection.close()
        

    def log_temperature_and_humidity(self, temp, moisture): 
        """
        Could undoubtably be written more generic
        but I will settle with this for now
        """

        connection = sqlite3.connect(self.filename)
        cursor     = connection.cursor()
        table      = clean_name(tablename)
        cursor.execute("INSERT INTO %s values(datetime('now'), (?), (?))"%table, (temp,moisture))
        connection.commit()
        connection.close()

    def store_measurements( self, tablename, data):     
        """
        The placeholder for the mentioned, more generic approach.
        """
        pass

    def display_data(self):    
        connection = sqlite3.connect(self.filename)
        cursor     = connection.cursor()
        string      = ""
        for row in cursor.execute("SELECT * FROM %s"%clean_name(self.tablename)):
            for value in row:
                string += str(value)+"\t"
            print(string+"\n")
        connection.close()    
            
class AM2302_sensor:
    def __init__(self, pin = 4 ):
        self.pin    =  pin
        self.sensor = Adafruit_DHT.AM2302
    def read_temp_and_humid(self): 
        humidity, temperature   = Adafruit_DHT.read_retry(self.sensor, self.pin)
        if humidity is not None and temperature is not None:
            self.humidity       = float(humidity)
            self.temperature    = float(temperature)
            return self.temperature,self.humidity 
        else:
            print("There was a read error")
            return None,None
            
# main function
# This is where the program starts 
def main():
    
    sensor = AM2302_sensor()

    temperature, humidity = sensor.read_temp_and_humid()
    while temperature == None:
        print("A read error: trying again")
        temperature, humidity = sensor.read_temp_and_humid(pin)
        # Maybe we should add a little bit of waiting time here?

    print ("temperature= %.1f C, humidity: %.1f %s"%(temperature,humidity, r"%") )

    # Store the perature in the database
    sensorlog = SensorLog(dbname,tablename)
    sensorlog.log_temperature_and_humidity(temperature,humidity)
        # lay the contents of the database
    sensorlog.display_data()

#        time.sleep(speriod)


if __name__=="__main__":
    main()
