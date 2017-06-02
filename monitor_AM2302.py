#!/usr/bin/python

import sqlite3

import os
import time
import glob

import Adafruit_DHT

# global variables
speriod=(15*60)-1
pad = '/home/hansdej/Sensorlog/rpi_temp_logger-master'
dbname=pad+'/am2303log.db'



# store the temperature in the database
def log_temperature_and_humidity(temp,moist):

    conn=sqlite3.connect(dbname)
    curs=conn.cursor()

    curs.execute("INSERT INTO readings values(datetime('now'), (?), (?))", (temp,moist))

    # commit the changes
    conn.commit()

    conn.close()


# display the contents of the database
def display_data():

    conn=sqlite3.connect(dbname)
    curs=conn.cursor()

    for row in curs.execute("SELECT * FROM readings"):
        print str(row[0])+"	"+str(row[1])

    conn.close()

# get temerature
# returns None on error, or the temperature as a float
def get_temp(devicefile):

    try:
        fileobj = open(devicefile,'r')
        lines = fileobj.readlines()
        fileobj.close()
    except:
        return None

    # get the status from the end of line 1 
    status = lines[0][-4:-1]

    # is the status is ok, get the temperature from line 2
    if status=="YES":
        print status
        tempstr= lines[1][-6:-1]
        tempvalue=float(tempstr)/1000
        print tempvalue
        return tempvalue
    else:
        print "There was an error."
        return None

def get_temp_and_humid(pin):
    sensor  = Adafruit_DHT.AM2302
    humidity, temperature = Adafruit_DHT.read_retry(sensor, pin)
    if humidity is not None and temperature is not None:
        temp = float(temperature)
        humid = float(humidity)
        return temp, humid
    else:
        print "There was an error."
        return None,None

        


# main function
# This is where the program starts 
def main():


#    while True:

    # get the temperature from the device file
    #temperature get_temp(w1devicefile)
    pin     = 4
    temperature, humidity = get_temp_and_humid(pin)
    if temperature != None:
        # print ("temperature= %.1f C, humidity: %.1f %s"%(temperature,humidity, r"%") )
        pass
    else:
        # Sometimes reads fail on the first attempt
        # so we need to retry
        temperature, humidity = get_temp_and_humid(pin)
        print ("temperature= %.1f C, humidity: %.1f %s"%(temperature,humidity, r"%") )

        # Store the temperature in the database
    log_temperature_and_humidity(temperature,humidity)

        # display the contents of the database
#        display_data()

#        time.sleep(speriod)


if __name__=="__main__":
    main()




