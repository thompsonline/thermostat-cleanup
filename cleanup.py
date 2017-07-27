#!/usr/bin/env python

import signal
import logging
import logging.handlers
import os
import sys
import ConfigParser
import time
import atexit
from signal import SIGTERM
import MySQLdb as mdb
import datetime
import mysql.connector
from mysql.connector import pooling

dname = os.path.dirname(os.path.abspath(__file__))

# read values from the config file
config = ConfigParser.ConfigParser()
config.read(dname + "/config.txt")

LOG_LOGFILE = config.get('logging', 'logfile')
logLevelConfig = config.get('logging', 'loglevel')
if logLevelConfig == 'info': 
    LOG_LOGLEVEL = logging.INFO
elif logLevelConfig == 'warn':
    LOG_LOGLEVEL = logging.WARNING
elif logLevelConfig ==  'debug':
    LOG_LOGLEVEL = logging.DEBUG   

LOGROTATE = config.get('logging', 'logrotation')
LOGCOUNT = int(config.get('logging', 'logcount'))

logger = logging.getLogger(__name__)
logger.setLevel(LOG_LOGLEVEL)
handler = logging.handlers.TimedRotatingFileHandler(LOG_LOGFILE, when=LOGROTATE, backupCount=LOGCOUNT)
formatter = logging.Formatter('%(asctime)s %(levelname)-8s %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

class MyLogger(object):
        def __init__(self, logger, level):
                self.logger = logger
                self.level = level

        def write(self, message):
                # Only log if there is a message (not just a new line)
                if message.rstrip() != "":
                        self.logger.log(self.level, message.rstrip())

sys.stdout = MyLogger(logger, logging.INFO)
sys.stderr = MyLogger(logger, logging.ERROR)

class ThermDatabase():
  _dbconfig = None
  _pool = None
  _logger = None
  
  def __init__(self, logger):
    self._dbconfig = {
      "database" : config.get('main', 'mysqlDatabase'),
      "host" : config.get('main', 'mysqlHost'),
      "user" : config.get('main', 'mysqlUser'),
      "password" : config.get('main', 'mysqlPass'), 
      "port" : int(config.get('main', 'mysqlPort'))	
    }
    self._logger = logger
  
  def connect(self):
    result = False
    if (self._pool == None):
      try:
        self._pool = mysql.connector.pooling.MySQLConnectionPool(pool_name="thermy", pool_size=3, **self._dbconfig)
        result = True
      except mdb.Error as err:
        self._logger.error("Database connect failed: %s" % (err))
      
    else:
      self._logger.warning("Database connect failed. Database already connected")
      result = True
      
    return result
    
  def getConnection(self):
    result = None
    if (self._pool != None):
      try:
        result = self._pool.get_connection()
      except mdb.Error as err:
        self._logger.error("Database pool allocation t failed: %s" % (err))
    else:
      self._logger.error("Unable to get database connection. Connect to database before requesting a pooled connection.")
    
    return result
    
  def getCursor(self, connection):
    result = None
    if (self._pool != None):
      if (connection != None):
        try:
          result = connection.cursor()
        except mdb.Error as err:
          self._logger.error("getCursor failed: %s" % (err))
      else:
        self._logger.error("Unable to get database connection. Connect to database before requesting a pooled connection.")
    else:
      self._logger.error("Unable to get database connection. Connect to database before requesting a pooled connection.")
      
    return result    

db = ThermDatabase(logger)
db.connect()


def cleanup(table, colname, name, timeframe):
  logger.info("%s cleanup starting" % name)
  conDB = db.getConnection()
  if (conDB != None):
    cursor = db.getCursor(conDB)
    if (cursor != None):
      diff = 0
      try:
        cursor.execute("SELECT COUNT(*) FROM %s" % table)
        count = int(cursor.fetchall()[0][0])
        cursor.execute("DELETE FROM %s WHERE %s < NOW() - INTERVAL %d DAY" % (table, colname, timeframe))
        cursor.execute("SELECT COUNT(*) FROM %s" % table)
        result = cursor.fetchall()
        diff = count - int(result[0][0])
        conDB.commit()
      except Exception as err:
        logger.error("Cleanup %s. %s" % (table, err))
      cursor.close()
    else:
      logger.error("Cleanup. Unable to get cursor.")
  
    conDB.close()
  
  logger.info("End cleanup of %s. %d rows deleted" % (name, diff))
        
cleanup("ControllerStatus", "lastStatus", "ControllerStatus", 90)
cleanup("SensorData", "timeStamp", "SensorData", 90)
cleanup("ThermostatLog", "timeStamp", "ThermostatLog", 90)
cleanup("Events", "timestamp", "Events", 90)
