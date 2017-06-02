#!/usr/bin/env python

import sqlite3

import os
import time
import glob
pad = '/home/hansdej/Sensorlog/rpi_temp_logger-master'
dbname=pad+'/am2302log.db'
dbname=pad+'/templog.db'

temp = 2.1e1
moist = 6.1e1
# store the temperature in the database
print(dbname)

if __name__=="__main__":
    # if False:

    conn=sqlite3.connect(dbname)
    curs=conn.cursor()

    curs.execute("INSERT INTO temps values(datetime('now'), (?))", (temp,))

    # commit the changes
    conn.commit()

    conn.close()
