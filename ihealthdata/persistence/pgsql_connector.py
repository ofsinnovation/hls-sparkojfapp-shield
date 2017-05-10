####################Database Access Layer ##############################################

import datetime
from calendar import timegm
from string import Template
import pandas as pd
import ihealthdata.utils.loggerutils as logger
#from ihealthdata.persistence.cassandra_error import CassandraError  # TODO find an equivalent Error logging for pgsql
from ihealthdata.utils.configmanager import ConfigManager
import sqlalchemy
#from sqlalchemy import Table, Column, Integer, String, ForeignKey
from sqlalchemy.sql import and_, or_, not_
import os
import psycopg2
from urlparse import urlparse


class PostgresConnector(object):

    def __init__(self):
        try:
            self.config_manager = ConfigManager()
            url = urlparse(os.environ["DATABASE_URL"])
            logger.info("URL ===>")
            logger.info(os.environ["DATABASE_URL"])
            self.user = url.username
            self.password = url.password
            self.db = url.path[1:]
            self.host = url.hostname
            self.port = url.port
            #self.conn, self.meta = self.connect(self.user, self.password, self.db, self.host, self.port)
            db_url = 'postgres://{}:{}@{}:{}/{}'
            db_url = db_url.format(self.user, self.password, self.host, self.port, self.db)
            self.conn = sqlalchemy.create_engine(db_url, client_encoding='utf8')
            self.meta = sqlalchemy.MetaData(bind=self.conn, reflect=True)
            logger.info("Connection established")

        except Exception as ex:
                logger.error('Exception Received while trying to connect to postgres')
                # raise OSError:
                #   print("cluster = Cluster(['10.0.0.12', '10.0.0.13', '10.0.0.14', '10.0.0.15'])", ex.__cause__")

    def connect(self, user, password, db, host, port):
        url = 'postgres://{}:{}@{}:{}/{}'
        url = url.format(user, password, host, port, db)
        logger.info("URL inside connect")
        logger.info(url)
        conn = sqlalchemy.create_engine(url, client_encoding='utf8')
        meta = sqlalchemy.MetaData(bind=self.conn, reflect=True)
        print('Connection established')
        logger.info("Connection established")
        return conn, meta


    def queryize(self, query):

        # try:
        clause = query
        #print(clause)
        #clause = results.select().where(results.c.year == 2005)

        #df = pd.read_sql(clause, con=self.connect(self.user, self.password, self.host, self.port, self.db))
        #clause = results.select().where(results.c.year == 2005)

                                                                   #, (activitymonitoringsimulatedanomaly.c.peopleid=109))
        #conn = self.connect(self.user, self.password, self.db, self.host)

        #return (df)

        '''    except Exception as ex:

            # logger.info('Exception occured while executing the query : ' + query)
            # raise CassandraError('Exception occured while executing the query : ' + query ) #, ex.with_traceback())
            print("getting error in " + query)
            ## Create and Send empty Pandas Dataframe
            return (pd.DataFrame())
        '''

    def data_access_producer(self, peopleid, index):

        peopleid = peopleid
        index = index
        query_template = Template(self.config_manager.config_item('queries', 'producer_query'))
        producer_query = query_template.safe_substitute(peopleid=peopleid, index=index)

        logger.debug('producer query : ' + producer_query)
        print("calling querize")

        activitymonitoringsimulatedanomaly = self.meta.tables['activitymonitoringsimulatedanomaly']

        clause = activitymonitoringsimulatedanomaly.select().where( \
            and_( \
                activitymonitoringsimulatedanomaly.c.seqno == index, \
                activitymonitoringsimulatedanomaly.c.peopleid == peopleid \
                ) \
            )

        print("getting DF")

        df = pd.read_sql(clause, self.conn)
        return  df



    def insert_activity_prediction(self, peopleid, activitydatetime, seqno, actualactivityid, \
                                   predictedactivityid, predictionaccuracy, heartrate):

        activity_prediction = self.meta.tables['activity_prediction']

        clause = activity_prediction.insert().values(peopleid=str(peopleid),
                                                     activitydatetime=str(activitydatetime), \
                                                     seqno=str(seqno), \
                                                     actualactivityid=str(actualactivityid), \
                                                     predictedactivityid=str(predictedactivityid), \
                                                     predictionaccuracy=str(predictionaccuracy), \
                                                     heartrate=str(heartrate))

        self.conn.execute(clause)

    def insert_activity_prediction_summary(self, peopleid, activitystartdatetime,  \
                                           activityenddatetime, seqno, activityid, persistentinminutes, avgheartrate, \
                                           minheartrate, maxheartrate, prediction_accuracy_subinterval):

        activity_prediction_summary = self.meta.tables['activity_prediction_summary']



        clause = activity_prediction_summary.insert().values(( \
            peopleid, activitystartdatetime, activityenddatetime, seqno, \
            activityid, persistentinminutes, avgheartrate, \
            minheartrate, maxheartrate, prediction_accuracy_subinterval))
        self.conn.execute(clause)

    # insert_cardiac_exception


    def insert_cardiac_exception(self, peopleid, exceptiondatetime, \
                                 seqno, activityid, isanomaly, heartrate, riskscore):


        activity_prediction = self.meta.tables['cardiac_exceptions']

        clause = activity_prediction.insert().values(peopleid=str(peopleid),
                                                     exceptiondatetime=str(exceptiondatetime), \
                                                     seqno=str(seqno), \
                                                     activityid=str(activityid), \
                                                     isanomaly=str(isanomaly), \
                                                     heartrate=str(heartrate), \
                                                     riskscore=str(riskscore))

        self.conn.execute(clause)

    def insert_cardiac_exception_summary(self, peopleid, exceptionstartdatetime, exceptionenddatetime, \
                                         seqno, activityid, persistentinminutes, avgheartrate, \
                                         minheartrate, maxheartrate, avgriskscore, maxriskscore):


        activity_prediction = self.meta.tables['cardiac_exceptions_summary']

        clause = activity_prediction.insert().values(peopleid=str(peopleid),
                                                     exceptionstartdatetime=str(exceptionstartdatetime), \
                                                     exceptionenddatetime=str(exceptionenddatetime), \
                                                     seqno=str(seqno), \
                                                     activityid=str(activityid), \
                                                     persistentinminutes=str(persistentinminutes), \
                                                     avgheartrate=str(avgheartrate), \
                                                     minheartrate=str(minheartrate), \
                                                     maxheartrate=str(maxheartrate), \
                                                     avgriskscore=str(avgriskscore), \
                                                     maxriskscore=str(maxriskscore))

        self.conn.execute(clause)

    def people_anomaly_stat_insert(self, peopleid, anomaly_status):
        dummy = "1111"

        activity_prediction = self.meta.tables['people']
        #activitymonitoringsimulatedanomaly = self.meta.tables['activitymonitoringsimulatedanomaly']

        clause = activity_prediction.update().values(dummy=str(dummy), \
                                                     anomaly_status=str(anomaly_status)).where(peopleid==str(peopleid))

        """clause = activitymonitoringsimulatedanomaly.select().where( \
            and_( \
                activitymonitoringsimulatedanomaly.c.seqno == index, \
                activitymonitoringsimulatedanomaly.c.peopleid == peopleid \
                ) \
            )"""
        self.conn.execute(clause)


    def activity_historical_data_access(self, peopleid):

        activitymonitoringhistorical = self.meta.tables['activitymonitoringhistorical']

        clause = activitymonitoringhistorical.select().where( \
            and_( \
                activitymonitoringhistorical.c.peopleid == peopleid \
                ) \
            )
       #   ).limit(1000)
        print("getting DF for peopleid = ", peopleid)
        print(clause)
        df = pd.read_sql(clause, self.conn)
        return df