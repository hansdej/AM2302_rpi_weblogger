#!/usr/bin/python3

simulate_sensor = False
import storage

import os
import time
import datetime
import glob
import logging
import sys

from storage import AM2302Reading
from os.path import expanduser



# global variables
speriod=(15*60)-1
pad = os.path.join(expanduser("~"),'PiSensor/AM2302_rpi_weblogger')
dbname=pad+'/newdata.db'
dbname=pad+'/am2302log.db'

class AM2302Sensor(object):
    """ The Adafruit AM2302 sensor object class.  """

    def __init__(self, *args, **kwargs):
        """
        Initialize it with all the keywords.
        """
        super(AM2302Sensor)
        # Zodat we ook op een sensorloos systeem kunnen testen.
        if 'pin' in kwargs:
            self.pin = kwargs['pin']
        else:
            self.pin = 4

        for kw,val in kwargs.items():
            setattr(self, kw, val)
        self.sensor = None

    def acquire_reading(self, *args, **kwargs):
        if 'stub' in args:
            # Testfase zonder sensor.
            if 't' in kwargs:
                temperature = kwargs['t']
            else:
                temperature = 21.21212121
            temperature = 21.21212121
            if 'h' in kwargs:
                humidity = kwargs['h']
            else:
                humidity = 50
            logging.debug("Stubbing humidity & temperature values")
        else:
            import Adafruit_DHT
            self.sensor = Adafruit_DHT.AM2302
            humidity, temperature = Adafruit_DHT.read_retry(
                                            self.sensor,
                                            self.pin)

        if humidity is not None and temperature is not None:
            self.temperature= float(temperature)
            self.humidity   = float(humidity)
            self.date       = datetime.datetime.now()
            self.saved      = False
        else:
            raise ValueError(
            'Missing temperature and/or humidity value. (%r,%r)'%(
                                            temperature,humidity))

    def store_reading(self, db_file_name):
        # Should:
        # 1. open a connection to the database
        # 2. Write the values: time is optional
        # 3. Close the database
        # Actually the adding and committing might be more
        # appropriate in the storage module.
        date        = self.date
        temperature = self.temperature
        humidity    = self.humidity
        #old_reading = storage.OldData(date, temperature, humidity)
        reading     = storage.AM2302Reading(date, temperature, humidity)
        reading.save_to_db(db_file_name)

        # Do the smoothing of the last 20 displayvalues.
        session = storage.connect(db_file_name)
        storage.update_displaylist(session, last=20)

# main function
# This is where the program starts
def main():
#    while True:

    # get the temperature from the device file
    #temperature get_temp(w1devicefile)
    sensor = AM2302Sensor(pin=4)
    if simulate_sensor == True:
        sensor.acquire_reading('stub',t=20, h = 48)
        sensor.store_reading('am2300log.db')
    else:
        sensor.acquire_reading()
        sensor.store_reading(dbname)

if __name__=="__main__":
    main()
