#!/usr/bin/env python3
"""
Deze moeten toegevoegd kunnen worden in scenario's:
    Bij de eerste conversie: de projecties van alle meetwaarde,
    Bij uitlezen sensoren: nieuwe records aanmaken en indien nodig oude
    records updaten.
"""
import sys
import os
import datetime
import logging
import logging.config
#from pandas import rolling_median

from sqlalchemy import Column, Integer, Float, String, Table, DateTime, NUMERIC
from sqlalchemy import create_engine, ForeignKey
from sqlalchemy import *
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import relationship
from sqlalchemy.orm import *
from sqlalchemy.ext.declarative import declarative_base

# 2 tabellen, een met de originele readings,
#           Een met de weergave waarden.
#
THEBASE = declarative_base()

logger = logging.getLogger()

def init_logging(logger):
    path = os.path.dirname(__file__)
    filename = os.path.join(path,"logging.ini")
    logging.config.fileConfig(filename, disable_existing_loggers=True)

def save_to_db(record, database, *args, **kwargs):
        """
        A method to write all declarative base elements to a database in the
        same way.
        """
        session = connect(database)
        session.add(record)
        session.commit()

# Add the function via a reference to THEBASE to make it behave like a method
# that its subclasses inherit.

#THEBASE.save_to_db = save_to_db


def get_records_around(session, record, then, number=5,
                       before=None, after=None):
    """
    Query for a number of records around a certain date.
    Obviously the records should contain a 'date' field.
    """
    # If 'before' and 'after' are not set, default to the configured 'number'
    before = number if before is None else before
    after  = number if after is None else after

    earlier = session.query(record).filter(record.date <= then
            ).order_by(record.date.asc()
            ).limit(before).subquery().select()

    later = session.query(record).filter(record.date > then
            ).order_by(record.date.asc()
            ).limit(after).subquery().select()

    the_union = union_all(earlier, later).alias()

    values = session.query(the_union).all()
    # We probably want to conserve the date's format since it is the identifier
    # to the corresponding database records.
    return values

# The layout of my existing database.
class OldData(THEBASE):
    """The Old data layout"""
    # Sqlitebrowser shows a _rowid_, is this really stored in the database or
    # is this a pseudovariable in sqlite?
    __tablename__ = 'readings'
    timestamp   = Column(DateTime,primary_key=True)
    temp        = Column(NUMERIC)
    moist       = Column(NUMERIC)

    def __init__(self,date,temperature,humidity):
        super(OldData,self).__init__()
        self.timestamp = date
        self.temp = temperature
        self.moist = humidity
    def __repr__(self):     
        mesg  = "<%r: %r "%(self.__class__,self.timestamp.ctime())
        mesg += ", (Temp, humid): (%.1f C, %.2f %% ) >"%(
                            self.temperature,self.humidity)
        return mesg

    def save_to_db(self, database):
        # Needs redefinition because I also want the display value saved here.
        save_to_db(self,database)

# The current database layout Readings and displayvalues in different tables.
# the only reason for this new table, is a link to processed display values,
# Strictly we can also rely only on a correction table if we want to save
# storage space, however this moves the processing penalty from acquisition
# time to retreival time again.
class AM2302Reading(THEBASE):
    """The new layout of the measurement data"""
    __tablename__='am2302readings'

    date        = Column(DateTime,primary_key=True)
    temperature = Column(Float)
    humidity    = Column(Float)
    # Link it to the table with display values.
    displayvalue= relationship('DisplayValue',
                                uselist        = False,
                                back_populates = 'reading')

    def __init__(self, date, temperature, humidity, **kwargs):
        self.date           = date
        self.temperature    = temperature
        self.humidity       = humidity
        # First we simply copy the data to the displayvalues table.


        if  'smooth' in kwargs and kwargs['smooth'] > 0:
            # add a displayvalue and apply the smoothing to the specified
            # number of datapoints before it; aranged according to the time
            # stamp
            pass
        else:
            # generate the Display value that belongs to this reading.
            # With the date referenced to this object, it is pretty uniquely
            # linked. An interpolation DisplayValue might be hard to achieve.
            # because it looks as if it needs a primary key from the readings
            # table.
            self.displayvalue   = DisplayValue(self,
                                               self.temperature,
                                               self.humidity)
    def __repr__(self):
        """
        Make the representation a bit more informative.
        """
        mesg  = "<%r: %r "%(self.__class__,self.date.ctime())
        mesg += ", (Temp, humid): (%.1f C, %.2f %% ) >"%(
                            self.temperature,self.humidity)
        return mesg

    def get_display_record(self,session):
        """
        Query the database for the display value that belongs to this reading.
        """
        display_value = session.query(DisplayValue).filter(

                    DisplayValue.date == allDates[i][0]).first()
        return display_value

    def save_to_db(self, database):
        # Needs redefinition because I also want the display value saved here.
        save_to_db(self,database)
        save_to_db(self.displayvalue,database)

class DisplayValue(THEBASE):
    """
    The table with the persistent display data:
    Primarily set up to smoothen outliers that occurred at the first sensor,
    but this enables a broader range of data processing tricks without changing
    (=corrupting) the original, raw data.
    """
    __tablename__ = 'displayvalues'

    #reading_id  = Column( Integer, ForeignKey('AM2302Readings.id'),
    #                                                primary_key=True )
    reading     = relationship('AM2302Reading', back_populates='displayvalue')

    date        = Column(   DateTime,
                            ForeignKey('am2302readings.date'),
                            primary_key=True
                        )
    # If we want to disconnect the displayValues from the sensor readings we
    # for example for interpolating or such, we must use another primary key
    # than the link to the date in the sensor readings.
    day         = Column(Float) # a day seems a good cadence measure.
    temperature = Column(Float)
    humidity    = Column(Float)

    def __init__(self, source_reading, temperature, humidity, **kwargs):
        """
        Initialisation of the DisplayValue.
        """
        def dateToDay( seconds):
            day = seconds/(24.0*3600)
            return day
        #self.reading_id = reading_id
        self.date       = source_reading.date
        self.day        = dateToDay(self.date.timestamp())
        self.temperature= temperature
        self.humidity   = humidity
    def __repr__(self):
        """
        Make the representation a bit more informative.
        """
        mesg  = "<%r: %r "%(self.__class__,self.date.ctime())
        mesg += ", (Temp, humid): (%.1f C, %.2f %% ) >"%(
                            self.temperature,self.humidity)
        return mesg

    #def save_to_db(self, database):
    #    session = connect(database)
    #    session.add(self)
    #    session.commit()


    def get_dB_record(self,session):
        """
        Query the database for the Measurement value that belongs to this reading.
        """
        sensor_reading = session.query(AM2302Reading).filter(
                    AM2302Reading.date == self.date).first()
        return sensor_reading

    def smooth(self, in_range=5):
        """
        Smooth the measurement entries in this row and a number of points around it.
        """
        # Eingenlijk is dit een kolom operatie, vooralsnog zie ik deze 'value'
        # als een rij.
        #
        # 1. query de laatste 5 voorliggende waarden obv datum volgorde.
        # 2. query de maximaal 5 nakomende waarden.
        # 3. Stop ze in volgorde in een numpy array.
        # 4. Bepaal of, en zo ja welke records een ongewone waarde hebben.
        # 5. Interpoleer vervangende waardes.
        # 6. Vervang de vreemde waardes in corresponderende records van de
        #     displaytabel.
        pass

oldDBname = 'oldformat.db'
newDBname = 'newformat.db'

def main(args):
    """
    The Main program does nothing.
    """
    return 1

def dburi(filename):
    """
    Complete the URI.
    """
    return "sqlite:///%s"%filename


# Display the contents of the database.
def fetch_daterange(dbname, start_date=None, end_data=None):
    """
    """
    session = connect(dbname)
    q = session.query(OldData).filter(
            OldData.timestamp >= start_date
            ).filter(OldData.timestamp <= end_date)
    measurements = []

    for r in q.all():
        measurements.append([r.timestamp, r.temp, r.moist ])

    print( "%r"%measurements)
    return measurements


# Dit is een gebruiksscript/functie.
#
def copy_old_to_new(oldFileName, newFileName, **kwargs):
    """ copy_old_to_new(oldDBname,newDBname)"""
    from shutil import copyfile
    try:
        os.remove(newFileName)
    except Exception as e:
        logger.warning("%r"%e)
    # Simply copy the old data to a new file: that way we will not mess with
    # the old and multiple files.
    # cost: we have to make sure that the new tables are empty.
    copyfile(oldFileName, newFileName)

    dB = dburi(newFileName)
    # Initialise the mechanisms to work with the old DB-layout:
    engine = create_engine(dB)
    THEBASE.metadata.bind = engine
    THEBASE.metadata.create_all()
    # Verwijder de tabellen met oude data en maak nieuwe aan. Ziet er uit als
    # een bewerkking die strikt genomen buiten het ORM principe ligt, maar het
    # werkt.
    for tabel in AM2302Reading, DisplayValue:
        try:
            tabel.__table__.drop(engine)
            tabel.__table__.create(engine)
        except Exception as e:
           logger.warning(e.message)

    dBsession = sessionmaker(bind=engine)
    session = dBsession()

    # Query de laatste ... weken:
    now   = datetime.datetime.now()
    begin = datetime.datetime.now()-datetime.timedelta(weeks=1)
    eind  = datetime.datetime.now()-datetime.timedelta(hours=0.5)
    eind  = datetime.datetime.now()

    q = session.query(OldData)
#    q = oldsess.query(OldData).filter(OldData.timestamp >= begin
#                               ).filter(OldData.timestamp <= eind)

    logger.info("start copying")
    cnt = 1
    start = datetime.datetime.now().timestamp()
    # 1 remove existing new tables
    # 2 initialize new tables
    # 3 add values from old readings
    for r in q.all():
        record = AM2302Reading( r.timestamp,
                                    r.temp,
                                    r.moist)
        session.add(record)
        session.add(record.displayvalue)
        cnt += 1
    added = datetime.datetime.now().timestamp()
    adding = added - start
    logger.info("Adding of %d records in %f s = %f s per record"%( cnt,
                                                                    adding,
                                                                    adding/cnt))
    logger.info("ready to commit %d records"%cnt)
    session.commit()
    committed = datetime.datetime.now().timestamp()
    committing = committed - added

    logger.info("Finished commit: in %f s: %f s per record"%(committing,
                                                         committing/cnt))
    return

import numpy as np


#def do_smooth (dbFileName):
# 1. Bepalen welke weergavewaardes vervangen moeten worden:
#       - Representatieve numpy array uitlezen uit de meetdata
#           met een goede vertalingstabel naar de database gegevens.
#       - bepalen welke entries vervangen moeten worden en waarmee.
#       - wissen van de afwijkende entry.
#       - Toevoegen van de verbeterde gegevens.
def determine_replacements(x,y,delta_level, copy=False):
        from scipy.signal import medfilt
        """
        Find extreme variations in x, return an array with their indices and
        accompanying, interpolated values.
        """
        #- Collect the indices of the elements that need to be interpolated.
        # Used to generate the mask of True or Falses when
        # the delta is bigger than the threshold d.

        # determine the difference between the data and a median filtered
        # version of it.
        smoothdeltas = lambda x :  x - medfilt(x)

        # Establish a mask of Trues or Falses with d as maximum variation
        masker = lambda x, d: np.abs(x) > np.ones(x.shape)*d

        # Mask the values where the median filtered 'replacement' differs
        # more than the delta_level.
        mask = masker(smoothdeltas(y), delta_level)

        # make a list of al indexes where the deviation was bigger than the
        # delta_level.
        badIs  = mask.nonzero()
        goodIs = (~mask).nonzero()
        goodYs = y[goodIs]
        logger.debug(
                "Interpolation of %d masked values"%len(badIs[0]) +\
                "of a total of %d."%len(mask))
        replacementYs = np.interp( x[badIs], x[goodIs], y[goodIs])
        if copy:
            returnedY = y.copy()
            returnedY[badIs] = replacementYs
            y = returnedY
        else:
            y[badIs] = replacementYs

        badIs_list = list(badIs[0]) # Simplify to a regular list.
        # return the indixes of the bad values together with the interpolated
        # replacement values.
        return badIs_list,y

def update_displaylist(db_filename, start=-5, **kwargs):
        """
        Cast a number of values from the sensor readings in an np.array, smooth
        this and write and replace them in the database's displaydata table.
        """
        last      = kwargs['end'] if 'end' in kwargs else None
        # Parse the session if it exists, else we open it.
        session = connect(db_filename)

        # En nu gaan we lussen
        # De kolommen uit de database in np arrays laden.
        # Er hoeft niets met de de id tabel gedaan te worden. Het lijkt wel nodig
        # om die te linken.
        
        # The [0] is needed to extract the value from the ORM-like object.
        allDates = [ d[0] for d in session.query(AM2302Reading.date).all()]
        # Gooi de data in np.arrays:
        dates   = np.array(allDates).squeeze()
        humid   = np.array(session.query(AM2302Reading.humidity).all()).squeeze()
        temp    = np.array(session.query(AM2302Reading.temperature).all()).squeeze()
        # use the linear timestamp, works in Python 3.
        times = np.array([date.timestamp() for date in dates]).squeeze()

        badIs, vals  = determine_replacements(times,temp,0.7)

        for i in badIs:
            record = session.query(DisplayValue).filter(
                    DisplayValue.date == allDates[i]).first()
            # Find the Displayvalue for it:
            oldValue = record.temperature
            record.temperature = vals[i]
            logger.debug("Record %d, T: %3f => %3f"%( i, oldValue,
                record.temperature))
        session.commit()
# Het vervangen lijkt te werken voor 2019, later maar eens kijken of het dingen
# goed gladstrijkt




        session.commit()

        # om de laatste 3 records te krijgen:
        # a = sess.query(AM2302Reading).order_by(AM2302Reading.id.desc()).limit(3)[::-1]

import matplotlib as mp
import matplotlib.pyplot as plt
#

#if True:
def connect(dbFileName):
    """
    Connect to an sqlite database file.
    """
    dburi  = "sqlite:///%s"%dbFileName
    engine = create_engine(dburi)
    THEBASE.metadata.bind=engine
    THEBASE.metadata.create_all()
    DBsession = sessionmaker(bind=engine)
    return DBsession()

if __name__ == '__main__':
    init_logging(logger)
    # read the database tables into the numpy arrays that are to be smoothed.
    #


    # Not via times: else it cannot be used in the database query.
    #begin = dates[0]
    #eind = dates[-1]
    #dates = np.array([d.timestamp()-end for d in dates])
    # from pandas import rolling_median,read_tablea

    # tempS = unspike_interpolate(times,temp,0.7)


    # That seemed to work: I assumed it would substitute the masked values
    # with interemediate values. The substitute value however appeared to
    # to be the all-over average.


    #return deltas
    # Fetch the old data in arrays.

    # Decide over the proper date format.
    # generate a smoothed version of the old data
    # add to the displaydata table
    # commit.
    #humid = np.array(sess.query(AM2302Reading.humidity).filter(
    #            AM2302Reading.date >= begin,AM2302Reading.date <= eind
    #                    ).all()).squeeze()
    #temp = np.array(sess.query(AM2302Reading.temperature).filter(
    #            AM2302Reading.date >= begin, AM2302Reading.date <= eind
    #                    ).all()).squeeze()
#
#
#

