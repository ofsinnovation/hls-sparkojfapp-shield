
'''
export PYTHONPATH=home/centos/spark-2.0.2-bin-hadoop2.7/python/lib/py4j-0.10.3-src.zip:/home/centos/spark-2.0.2-bin-hadoop2.7/python:/home/centos/spark-2.0.2-bin-hadoop2.7/python/build:/home/centos/release1_5/

spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.1,TargetHolding/pyspark-cassandra:0.3.5     /home/centos/release1_5/ihealthdata/consumer/kaustav_ihealth_anomaly_detection.py
'''



import math
#from __future__ import print_function
from pyspark import SparkContext,SparkConf,SQLContext
from pyspark.streaming import StreamingContext
# from cassandra.cluster import Cluster
from datetime import datetime
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
import pandas as pd
import numpy as np
import scipy.sparse as sparse
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SparkSession
import random
from pyspark.streaming.kafka import KafkaUtils
from ihealthdata.helper.HelperVariable import *
#from ihealthdata.models.Spark_MLLib_Activity_Predict.TestPointCreation import TestPointCreation
from ihealthdata.utils.SwitchCase import SwitchCase
from ihealthdata.persistence.pgsql_connector import PostgresConnector
from ihealthdata.utils.Encoder import * #Encode_Label
import ihealthdata.utils.loggerutils as logger
from ihealthdata.consumer.ParserDecorator import ParserDecorator
from ihealthdata.messaging.kafka_helper import  get_kafka_brokers, get_kafka_consumer, get_zk_list
import sys


sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject101')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject102')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject103')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject104')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject105')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject106')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject107')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject108')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject109')


from ihealthdata.models.Numenta_Anomaly_Detect.Subject101.Subject101_Run import model_Result_101, create_Numenta_Model_101
from ihealthdata.models.Numenta_Anomaly_Detect.Subject102.Subject102_Run import model_Result_102, create_Numenta_Model_102
from ihealthdata.models.Numenta_Anomaly_Detect.Subject103.Subject103_Run import model_Result_103, create_Numenta_Model_103
from ihealthdata.models.Numenta_Anomaly_Detect.Subject104.Subject104_Run import model_Result_104, create_Numenta_Model_104
from ihealthdata.models.Numenta_Anomaly_Detect.Subject105.Subject105_Run import model_Result_105, create_Numenta_Model_105
from ihealthdata.models.Numenta_Anomaly_Detect.Subject106.Subject106_Run import model_Result_106, create_Numenta_Model_106
from ihealthdata.models.Numenta_Anomaly_Detect.Subject107.Subject107_Run import model_Result_107, create_Numenta_Model_107
from ihealthdata.models.Numenta_Anomaly_Detect.Subject108.Subject108_Run import model_Result_108, create_Numenta_Model_108
from ihealthdata.models.Numenta_Anomaly_Detect.Subject109.Subject109_Run import model_Result_109, create_Numenta_Model_109

##################### END OF ALL IMPORTS ################################################################

## Ganapathy - How can we move this within the class, the lambda function just won't take objects
############################################################################
def LabelPoint_Creation(l):

        l = [float(x) for x in l]
        return LabeledPoint(l[0],l[1:])

#################################################################################

def LabelPoint_Preparation(data):

        test_df = data.map(lambda l_data: LabelPoint_Creation(l_data[0]))
        return(test_df)
#######################################################################################

class Consumer(object):

	def __init__(self):
		self.hv = HelperVariable()
		self.ACTIVITY_ENCODER_DICTIONARY = self.hv.ACTIVITY_ENCODER_DICTIONARY_INITIALIZER

		self.deco = ParserDecorator()
		self.conn = PostgresConnector()

		self.Row_Index = 0
		####################################################################################
		self.CORRECT_PREDICTED_ACTIVITY_COUNTER = self.hv.CORRECT_PREDICTED_ACTIVITY_COUNTER
		self.PREDICTION_ACCURACY = self.hv.PREDICTION_ACCURACY

		self.START_TIME_NEW_SUMMARY_DICTIONARY = self.hv.START_TIME_NEW_SUMMARY_DICTIONARY_INITIALIZER

		##############################################################################################
		global create_Numenta_Model_101, create_Numenta_Model_102, create_Numenta_Model_103, \
			create_Numenta_Model_104, create_Numenta_Model_105, create_Numenta_Model_106, \
			create_Numenta_Model_107, create_Numenta_Model_108, create_Numenta_Model_109

		self.model_101 = create_Numenta_Model_101()
		self.model_102 = create_Numenta_Model_102()
		self.model_103 = create_Numenta_Model_103()
		self.model_104 = create_Numenta_Model_104()
		self.model_105 = create_Numenta_Model_105()
		self.model_106 = create_Numenta_Model_106()
		self.model_107 = create_Numenta_Model_107()
		self.model_108 = create_Numenta_Model_108()
		self.model_109 = create_Numenta_Model_109()

		############################################################################
		self.ACTIVITY_PERSISTENCE_DICTIONARY = self.hv.ACTIVITY_PERSISTENCE_DICTIONARY

		self.MAX_HEARTRATE_DICTIONARY = self.hv.MAX_HEARTRATE_DICTIONARY

		self.MIN_HEARTRATE_DICTIONARY = self.hv.MIN_HEARTRATE_DICTIONARY

		self.SUMMARY_TRANSITION_INDICATOR_FLAG_DICTIONARY = self.hv.SUMMARY_TRANSITION_INDICATOR_FLAG_DICTIONARY

		self.ACTIVITY_PERSISTENCE_COUNTER_DICTIONARY = self.hv.ACTIVITY_PERSISTENCE_COUNTER_DICTIONARY

		self.SUM_HEARTRATE_DICTIONARY = self.hv.SUM_HEARTRATE_DICTIONARY

		self.SUM_RISK_SCORE_DICTIONARY = self.hv.SUM_RISK_SCORE_DICTIONARY

		self.ANOMALY_SCORE_MAX_DICTIONARY = self.hv.ANOMALY_SCORE_MAX_DICTIONARY

		self.PREDICTION_ACCURACY_SUB_INTERVAL_DICTIONARY = self.hv.PREDICTION_ACCURACY_SUB_INTERVAL_DICTIONARY

		self.SUM_CORRECT_PREDICTED_ACTIVITY_COUNTER_DICTIONARY = self.hv.SUM_CORRECT_PREDICTED_ACTIVITY_COUNTER_DICTIONARY

		self.START_ACTIVITY_DICTIONARY = self.hv.START_ACTIVITY_DICTIONARY

		self.AVERAGE_HEARTRATE_DICTIONARY = self.hv.AVERAGE_HEARTRATE_DICTIONARY

		self.AVG_RISK_SCORE_DICTIONARY = self.hv.AVG_RISK_SCORE_DICTIONARY

		################################################################################################

		## Ganapathy - Is there a better way to do this empty Dictionary declaration. I dont like putting empty
		## string quotes
		self.ACTIVITY_PREDICTION_MODEL_DICTIONARY = {   \
		101:'', \
		102:'', \
		103:'', \
		104:'', \
		105:'', \
		106:'', \
		107:'', \
		108:'', \
		109:'', \
		}

		#################################################################################################
		## Setting-up Spark Configurations
		self.spark_configurations()

		# self.data_access_layer = PostgresConnector()

		# self.test_point_creation = TestPointCreation()

		self.enc = Encoder()

		self.activity_prediction_model_creation()

	## Setting up Spark Configurations
	def spark_configurations(self):

		'''
		self.conf = (SparkConf()                            \
        '''

		# Setting up Spark-Context
		self.sc = SparkContext()
		
		#Setting up Spark-SQL-Context
		self.sqlContext = SQLContext(self.sc)

		# Setting up Spark-Streaming-Context with Batch of 10 secs
		self.ssc = StreamingContext(self.sc, self.hv.SPARK_STREAMING_CONTEXT_DURATION)


	## Currently the Model Persistence thing (save to disk and read it back) is not working
	## We need to fix this


	def activity_prediction_model_persistence_load(self):
		self.activity_prediction_model_101 = DecisionTreeModel.load(self.sc, self.hv.ACTIVITY_MODEL_PATH_101)
		self.activity_prediction_model_102 = DecisionTreeModel.load(self.sc, self.hv.ACTIVITY_MODEL_PATH_102)
		self.activity_prediction_model_103 = DecisionTreeModel.load(self.sc, self.hv.ACTIVITY_MODEL_PATH_103)
		self.activity_prediction_model_104 = DecisionTreeModel.load(self.sc, self.hv.ACTIVITY_MODEL_PATH_104)
		self.activity_prediction_model_105 = DecisionTreeModel.load(self.sc, self.hv.ACTIVITY_MODEL_PATH_105)
		self.activity_prediction_model_106 = DecisionTreeModel.load(self.sc, self.hv.ACTIVITY_MODEL_PATH_106)
		self.activity_prediction_model_107 = DecisionTreeModel.load(self.sc, self.hv.ACTIVITY_MODEL_PATH_107)
		self.activity_prediction_model_108 = DecisionTreeModel.load(self.sc, self.hv.ACTIVITY_MODEL_PATH_108)
		self.activity_prediction_model_109 = DecisionTreeModel.load(self.sc, self.hv.ACTIVITY_MODEL_PATH_109)

	## This is to circumvent the problem of non-working Model-Persistence code

	def activity_prediction_model_preparation(self, peopleid):

		pandas_df = self.conn.activity_historical_data_access(peopleid)
		print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  " + str(peopleid))

		spark_df = self.sqlContext.createDataFrame(pandas_df)
		ACTIVITY_ENCODER_DICTIONARY = self.ACTIVITY_ENCODER_DICTIONARY
		df_dataset = spark_df.rdd.map(lambda row: LabeledPoint\
                     			(ACTIVITY_ENCODER_DICTIONARY[row.activityid],
						 [ float(row.heartrate),       \
											 float(row.imuankleacc16gxaxis), \
											 float(row.imuankleacc16gyaxis), \
											 float(row.imuankleacc16gzaxis), \
											 float(row.imuankleacc6gxaxis),  \
											 float(row.imuankleacc6gyaxis),  \
											 # float(imuankleacc6gzaxis),  \
											 float(row.imuanklegyroxaxis),   \
											 float(row.imuanklegyroyaxis),   \
											 float(row.imuanklegyrozaxis),   \
											 float(row.imuanklemagxaxis),    \
											 float(row.imuanklemagyaxis),    \
											 float(row.imuanklemagzaxis),    \
											 float(row.imuankleori1),        \
											 float(row.imuankleori2),        \
											 float(row.imuankleori3),        \
											 float(row.imuankleori4),        \
											 float(row.imuankletemp),        \
											 float(row.imuchestacc16gxaxis), \
											 float(row.imuchestacc16gyaxis), \
											 float(row.imuchestacc16gzaxis), \
											 float(row.imuchestacc6gxaxis),  \
											 float(row.imuchestacc6gyaxis),  \
											 float(row.imuchestacc6gzaxis),  \
											 float(row.imuchestgyroxaxis),   \
											 float(row.imuchestgyroyaxis),   \
											 float(row.imuchestgyrozaxis),   \
											 float(row.imuchestmagxaxis),    \
											 float(row.imuchestmagyaxis),    \
											 float(row.imuchestmagzaxis),    \
											 float(row.imuchestori1),        \
											 float(row.imuchestori2),        \
											 float(row.imuchestori3),        \
											 float(row.imuchestori4),        \
											 float(row.imuchesttemp),        \
											 float(row.imuhandacc16gxaxis),  \
											 float(row.imuhandacc16gyaxis),  \
											 float(row.imuhandacc16gzaxis),  \
											 float(row.imuhandacc6gxaxis),   \
											 float(row.imuhandacc6gyaxis),   \
											 float(row.imuhandacc6gzaxis),   \
											 float(row.imuhandgyroxaxis),    \
											 float(row.imuhandgyroyaxis),    \
											 float(row.imuhandgyrozaxis),    \
											 float(row.imuhandmagxaxis),     \
											 float(row.imuhandmagyaxis),     \
											 float(row.imuhandmagzaxis),     \
											 float(row.imuhandori1),         \
											 float(row.imuhandori2),         \
											 float(row.imuhandori3),         \
											 float(row.imuhandori4),         \
											 float(row.imuhandtemp)]))


		# (self.hv.ACTIVITY_ENCODER_DICTIONARY_INITIALIZER[row.activityid], [row.heartrate,row.imuhandtemp]))
		## Need to see if we can do some nested lambdas to make this work

		(trainingData, testData) = df_dataset.randomSplit([1.0, 0.0])

		# Ganapathy - The models should be ideally in a dictionary like this. How do I make this to work.
		# String initialization of model dictionary is messing things up


		""""self.ACTIVITY_PREDICTION_MODEL_DICTIONARY[peopleid] = DecisionTree.trainClassifier( \
					trainingData, numClasses = self.hv.NUM_CLASSES, categoricalFeaturesInfo={}, \
					impurity = self.hv.IMPURITY, maxDepth = self.hv.MAX_DEPTH, maxBins = self.hv.MAX_BINS)"""

		#################################################################################################
		
		num_classes = self.hv.NUM_CLASSES
		impurity = self.hv.IMPURITY
		max_depth = self.hv.MAX_DEPTH
		max_bins = self.hv.MAX_BINS

		if(peopleid == 101):

			self.activity_detection_model_101 = DecisionTree.trainClassifier( \
							trainingData, numClasses = num_classes, categoricalFeaturesInfo={}, \
							impurity = impurity, maxDepth = max_depth, maxBins = max_bins)

		if(peopleid == 102):

				self.activity_detection_model_102 = DecisionTree.trainClassifier( \
								trainingData, numClasses = num_classes, categoricalFeaturesInfo={}, \
								impurity = impurity, maxDepth = max_depth, maxBins = max_bins)

		if(peopleid == 103):

				self.activity_detection_model_103 = DecisionTree.trainClassifier( \
								trainingData, numClasses = num_classes, categoricalFeaturesInfo={}, \
								impurity = impurity, maxDepth = max_depth, maxBins = max_bins)

		if(peopleid == 104):

				self.activity_detection_model_104 = DecisionTree.trainClassifier( \
								trainingData, numClasses = num_classes, categoricalFeaturesInfo={}, \
								impurity = impurity, maxDepth = max_depth, maxBins = max_bins)

		if(peopleid == 105):

				self.activity_detection_model_105 = DecisionTree.trainClassifier( \
								trainingData, numClasses = num_classes, categoricalFeaturesInfo={}, \
								impurity = impurity, maxDepth = max_depth, maxBins = max_bins)

		if(peopleid == 106):

				self.activity_detection_model_106 = DecisionTree.trainClassifier( \
								trainingData, numClasses = num_classes, categoricalFeaturesInfo={}, \
								impurity = impurity, maxDepth = max_depth, maxBins = max_bins)

		if(peopleid == 107):

				self.activity_detection_model_107 = DecisionTree.trainClassifier( \
								trainingData, numClasses = num_classes, categoricalFeaturesInfo={}, \
								impurity = impurity, maxDepth = max_depth, maxBins = max_bins)

		if(peopleid == 108):

				self.activity_detection_model_108 = DecisionTree.trainClassifier( \
								trainingData, numClasses = num_classes, categoricalFeaturesInfo={}, \
								impurity = impurity, maxDepth = max_depth, maxBins = max_bins)

		if(peopleid == 109):

				self.activity_detection_model_109 = DecisionTree.trainClassifier( \
								trainingData, numClasses = num_classes, categoricalFeaturesInfo={}, \
								impurity = impurity, maxDepth = max_depth, maxBins = max_bins)


		####################################################################################################

		# SwitchCase required. Have to bring this to working condition
	############################################################################################################
	def parser(self,x):


		len_val = len(x)
		token_index = x.index(" ")
		val_str = x[(token_index+1):(len_val)]
		return (float(val_str))

	#####################################################################################

	def activity_prediction_model_creation(self):

		map(self.activity_prediction_model_preparation, self.hv.SUBJECT_LIST_ID)

	#############################################################################
	############ SUMMARY TABLE UPDATION LOGIC ###################################
	def Summary_Table_Initialization(self,people_id):


		self.ACTIVITY_PERSISTENCE_DICTIONARY[people_id] = 1
		self.MAX_HEARTRATE_DICTIONARY[people_id] = 0
		self.MIN_HEARTRATE_DICTIONARY[people_id] = 300
		self.SUMMARY_TRANSITION_INDICATOR_FLAG_DICTIONARY[people_id] = 0

		self.ACTIVITY_PERSISTENCE_COUNTER_DICTIONARY[people_id] = 1
		self.SUM_HEARTRATE_DICTIONARY[people_id] = 0
		self.SUM_RISK_SCORE_DICTIONARY[people_id] = 0
		self.ANOMALY_SCORE_MAX_DICTIONARY[people_id] = 0
		self.PREDICTION_ACCURACY_SUB_INTERVAL_DICTIONARY[people_id] = 0
		self.SUM_CORRECT_PREDICTED_ACTIVITY_COUNTER_DICTIONARY[people_id] = 0

		self.AVERAGE_HEARTRATE_DICTIONARY[people_id] = 0
		self.START_TIME_NEW_SUMMARY_DICTIONARY[people_id] = datetime.now()
	############################################################################################


	def Summary_Table_Updation(self,timestamp, heartbeat, people_id, seqno, activity_id):

		print(people_id, \
		self.SUM_HEARTRATE_DICTIONARY[people_id], \
		self.ACTIVITY_PERSISTENCE_COUNTER_DICTIONARY[people_id])

		print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

		if(self.SUMMARY_TRANSITION_INDICATOR_FLAG_DICTIONARY[people_id]):

			persistence_counter = int(self.ACTIVITY_PERSISTENCE_COUNTER_DICTIONARY[people_id])
			self.AVERAGE_HEARTRATE_DICTIONARY[people_id] = \
				int(self.SUM_HEARTRATE_DICTIONARY[people_id] / (persistence_counter -1))

			self.AVG_RISK_SCORE_DICTIONARY[people_id] = \
				(self.SUM_RISK_SCORE_DICTIONARY[people_id] / self.ACTIVITY_PERSISTENCE_COUNTER_DICTIONARY[people_id])

			self.PREDICTION_ACCURACY_SUB_INTERVAL_DICTIONARY[people_id] = \
					float((self.SUM_CORRECT_PREDICTED_ACTIVITY_COUNTER_DICTIONARY[people_id] \
						 / self.ACTIVITY_PERSISTENCE_COUNTER_DICTIONARY[people_id])*100)

			if(self.ANOMALY_SCORE_MAX_DICTIONARY[people_id] > 0.8):

				self.conn.insert_cardiac_exception_summary(int(people_id), \
                                				self.START_TIME_NEW_SUMMARY_DICTIONARY[people_id], \
                                				timestamp, \
                                				int(seqno), \
                                				activity_id, \
                                				self.ACTIVITY_PERSISTENCE_DICTIONARY[people_id], \
                                				self.AVERAGE_HEARTRATE_DICTIONARY[people_id], \
                                				self.MIN_HEARTRATE_DICTIONARY[people_id], \
                                				self.MAX_HEARTRATE_DICTIONARY[people_id], \
                                				self.AVG_RISK_SCORE_DICTIONARY[people_id], \
                                				self.ANOMALY_SCORE_MAX_DICTIONARY[people_id])



				self.conn.insert_activity_prediction_summary(int(people_id),        \
							self.START_TIME_NEW_SUMMARY_DICTIONARY[people_id],  \
                                  			timestamp,                                       \
							int(seqno),                                           \
							int(activity_id),                                     \
							self.ACTIVITY_PERSISTENCE_DICTIONARY[people_id], \
				                        self.AVERAGE_HEARTRATE_DICTIONARY[people_id],    \
                                  			self.MIN_HEARTRATE_DICTIONARY[people_id],        \
							self.MAX_HEARTRATE_DICTIONARY[people_id],        \
							self.PREDICTION_ACCURACY_SUB_INTERVAL_DICTIONARY[people_id])



				# self.START_ACTIVITY_DICTIONARY[people_id] = activity_id
				## Initialize Summary Parameters
				self.Summary_Table_Initialization(people_id)

                #################################################################################################

	def Numenta_Results(self,timestamp, heartbeat, people_id):

		global model_Result_102, model_Result_103, model_Result_104, model_Result_105, model_Result_106, \
			model_Result_107, model_Result_108, model_Result_109

		if(people_id == 101):


			numenta_model = self.model_102

			output_timestamp, heartbeat, prediction, anomalyScore = \
										model_Result_102(timestamp, heartbeat, numenta_model)

			return(output_timestamp, heartbeat, prediction, anomalyScore)

		if(people_id == 102):

				numenta_model = self.model_102

				output_timestamp, heartbeat, prediction, anomalyScore = \
										model_Result_102(timestamp, heartbeat, numenta_model)

				return(output_timestamp, heartbeat, prediction, anomalyScore)

		if(people_id == 103):

				numenta_model = self.model_103

				output_timestamp, heartbeat, prediction, anomalyScore = \
										model_Result_103(timestamp, heartbeat, numenta_model)

				return(output_timestamp, heartbeat, prediction, anomalyScore)

		if(people_id == 104):

				numenta_model = self.model_104

				output_timestamp, heartbeat, prediction, anomalyScore = \
										model_Result_104(timestamp, heartbeat, numenta_model)

				return(output_timestamp, heartbeat, prediction, anomalyScore)

		if(people_id == 105):

				numenta_model = self.model_105

				output_timestamp, heartbeat, prediction, anomalyScore = \
										model_Result_105(timestamp, heartbeat, numenta_model)

				return(output_timestamp, heartbeat, prediction, anomalyScore)

		if(people_id == 106):

				numenta_model = self.model_106

				output_timestamp, heartbeat, prediction, anomalyScore = \
										model_Result_106(timestamp, heartbeat, numenta_model)

				return(output_timestamp, heartbeat, prediction, anomalyScore)

		if(people_id == 107):

				numenta_model = self.model_107

				output_timestamp, heartbeat, prediction, anomalyScore = \
										model_Result_107(timestamp, heartbeat, numenta_model)

				return(output_timestamp, heartbeat, prediction, anomalyScore)

		if(people_id == 108):

				numenta_model = self.model_108

				output_timestamp, heartbeat, prediction, anomalyScore = \
										model_Result_108(timestamp, heartbeat, numenta_model)

				return(output_timestamp, heartbeat, prediction, anomalyScore)

		if(people_id == 109):

				numenta_model = self.model_109

				output_timestamp, heartbeat, prediction, anomalyScore = \
										model_Result_109(timestamp, heartbeat, numenta_model)

				return(output_timestamp, heartbeat, prediction, anomalyScore)


		'''

		for case in SwitchCase(people_id):

                	if case(101):
				numenta_model = self.model_102
				output_timestamp, heartbeat, prediction, anomalyScore = \
						self.model_Result_102(timestamp, heartbeat, numenta_model)
				return(output_timestamp, heartbeat, prediction, anomalyScore)

			if case(102):
				numenta_model = self.model_102
                                output_timestamp, heartbeat, prediction, anomalyScore = \
					self.model_Result_102(timestamp, heartbeat, numenta_model)
                                return(output_timestamp, heartbeat, prediction, anomalyScore)

			if case(103):
				numenta_model = self.model_103
                                output_timestamp, heartbeat, prediction, anomalyScore = \
						self.model_Result_103(timestamp, heartbeat, numenta_model)
                                return(output_timestamp, heartbeat, prediction, anomalyScore)

			if case(104):
				numenta_model = self.model_104
                                output_timestamp, heartbeat, prediction, anomalyScore = \
					self.model_Result_104(timestamp, heartbeat, numenta_model)
                                return(output_timestamp, heartbeat, prediction, anomalyScore)

			if case(105):
				numenta_model = self.model_105
                                output_timestamp, heartbeat, prediction, anomalyScore = \
					self.model_Result_105(timestamp, heartbeat, numenta_model)
                                return(output_timestamp, heartbeat, prediction, anomalyScore)

			if case(106):
				numenta_model = self.model_106
                                output_timestamp, heartbeat, prediction, anomalyScore = \
					self.model_Result_106(timestamp, heartbeat, numenta_model)
                                return(output_timestamp, heartbeat, prediction, anomalyScore)

			if case(107):
				numenta_model = self.model_107
                                output_timestamp, heartbeat, prediction, anomalyScore = \
					self.model_Result_107(timestamp, heartbeat, numenta_model)
                                return(output_timestamp, heartbeat, prediction, anomalyScore)

			if case(108):
				numenta_model = self.model_108
                                output_timestamp, heartbeat, prediction, anomalyScore = \
					self.model_Result_108(timestamp, heartbeat, numenta_model)
                                return(output_timestamp, heartbeat, prediction, anomalyScore)

			if case(109):
				numenta_model = self.model_109
                                output_timestamp, heartbeat, prediction, anomalyScore = \
					self.model_Result_109(timestamp, heartbeat, numenta_model)
                                return(output_timestamp, heartbeat, prediction, anomalyScore)

		'''

	#####################################################################################
	def Numenta_Operations(self, current_time, people_id, actual_activity_id, \
			             activity_id, timestamp, heartbeat, seqno):

		############################################################################################
		output_timestamp, heartbeat, prediction, anomalyScore = \
				self.Numenta_Results(timestamp, heartbeat, people_id)

		print(output_timestamp, heartbeat, prediction, anomalyScore)
		print("########################################################")

		if(anomalyScore > 0.8):

			isanomaly = "RED"
		else:
			isanomaly = "GREEN"
		## Introducing Jitter to make it more life-like
		if(anomalyScore < 0.1):

			jitter = random.uniform(0.0, 0.3)
			anomalyScore = anomalyScore + jitter

			self.conn.insert_cardiac_exception(int(people_id), output_timestamp,        	     \
																	int(seqno),                        \
																	activity_id,                  \
																	isanomaly,		      \
										heartbeat,           	      \
											anomalyScore)

		############################################################################################
		# Updating Anomaly Status in people table
		# Partition key is dummy
		if (isanomaly == "RED" and (people_id == 109 or people_id == 107)):

			anomaly_status = "RED"
			self.conn.people_anomaly_stat_insert(int(people_id), anomaly_status)

		# Summary Code Logic
		if(heartbeat > self.MAX_HEARTRATE_DICTIONARY[people_id]) :
                	self.MAX_HEARTRATE_DICTIONARY[people_id] = heartbeat

		##################################################################################################

		if(heartbeat < self.MIN_HEARTRATE_DICTIONARY[people_id]):
                	self.MIN_HEARTRATE_DICTIONARY[people_id] = heartbeat

		#################################################################################################

		if(anomalyScore > self.ANOMALY_SCORE_MAX_DICTIONARY[people_id]):
                	self.ANOMALY_SCORE_MAX_DICTIONARY[people_id] = anomalyScore

		###################################################################################################

		self.SUM_HEARTRATE_DICTIONARY[people_id] = self.SUM_HEARTRATE_DICTIONARY[people_id] + heartbeat

		###########################################################################################

		self.SUM_RISK_SCORE_DICTIONARY[people_id] = self.SUM_RISK_SCORE_DICTIONARY[people_id] + anomalyScore

		#####################################################################################################
		self.ACTIVITY_PERSISTENCE_COUNTER_DICTIONARY[people_id] += 1
		############################################################################################


		if(activity_id == actual_activity_id):
                	self.SUM_CORRECT_PREDICTED_ACTIVITY_COUNTER_DICTIONARY[people_id] += 1

		###################################################################################################
		# current_time = datetime.now()
		duration = (current_time - self.START_TIME_NEW_SUMMARY_DICTIONARY[people_id]).seconds
		dictionary_activity_id = self.START_ACTIVITY_DICTIONARY[people_id]
		dictionary_activity_id = dictionary_activity_id.encode('ascii','ignore')


		# print(duration,self.hv.SUMMARY_POLLING_DURATION)
		# print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
		# print(people_id, seqno, float(dictionary_activity_id), activity_id)
		# print(type(activity_id))

		##################################################################################################

		if(duration > self.hv.SUMMARY_POLLING_DURATION or float(dictionary_activity_id) != activity_id):
		# if(float(dictionary_activity_id) != activity_id):
		# if((float(dictionary_activity_id) != activity_id) and people_id == 109):

			print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$  CHECK HERE   $$$$$$$$$$$$$$$$$$$$$$$$")
			print(duration,self.hv.SUMMARY_POLLING_DURATION)
			print("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$  CHECK HERE   $$$$$$$$$$$$$$$$$$$$$$$$")

			print("^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^")
			print(people_id, seqno, float(dictionary_activity_id), activity_id)
			print(type(activity_id))

			print(int(self.SUM_HEARTRATE_DICTIONARY[people_id]),  \
				self.ACTIVITY_PERSISTENCE_COUNTER_DICTIONARY[people_id])

			print (int(self.SUM_HEARTRATE_DICTIONARY[people_id] /\
				self.ACTIVITY_PERSISTENCE_COUNTER_DICTIONARY[people_id]))
			self.ACTIVITY_PERSISTENCE_DICTIONARY[people_id] = duration

			self.SUMMARY_TRANSITION_INDICATOR_FLAG_DICTIONARY[people_id] = 1

			# self.START_TIME_NEW_SUMMARY_DICTIONARY[people_id] = datetime.now()

			# Summary Table Updation
			self.Summary_Table_Updation(current_time, heartbeat, people_id, seqno, activity_id)

		else:
                	self.SUMMARY_TRANSITION_INDICATOR_FLAG_DICTIONARY[people_id] = 0

					#	self.ACTIVITY_PERSISTENCE_COUNTER_DICTIONARY[people_id] += 1

                    # ################################


	def Activity_Prediction_Results(self, test_df, peopleid):

		# Ganapathy - We need to make this dictionary work
		# self.ACTIVITY_PREDICTION_MODEL_DICTIONARY[peopleid].predict(test_df.map(lambda x: x.features))

		if(peopleid == 101):

			prediction = self.activity_detection_model_101.predict(test_df.map(lambda x: x.features))
			return(prediction)

		if(peopleid == 102):

			prediction = self.activity_detection_model_102.predict(test_df.map(lambda x: x.features))
			return(prediction)

		if(peopleid == 103):

			prediction = self.activity_detection_model_103.predict(test_df.map(lambda x: x.features))
			return(prediction)

		if(peopleid == 104):

			prediction = self.activity_detection_model_104.predict(test_df.map(lambda x: x.features))
			return(prediction)

		if(peopleid == 105):

			prediction = self.activity_detection_model_105.predict(test_df.map(lambda x: x.features))
			return(prediction)

		if(peopleid == 106):

			prediction = self.activity_detection_model_106.predict(test_df.map(lambda x: x.features))
			return(prediction)

		if(peopleid == 107):

			prediction = self.activity_detection_model_107.predict(test_df.map(lambda x: x.features))
			return(prediction)

		if(peopleid == 108):

			prediction = self.activity_detection_model_108.predict(test_df.map(lambda x: x.features))
			return(prediction)

		if(peopleid == 109):

			prediction = self.activity_detection_model_109.predict(test_df.map(lambda x: x.features))
			return(prediction)



	####################################################################################################
	'''
	def data_parameter_extraction(self, pdf, current_stream_index, column_index, column_name, type_parser):

		data_column = pdf.iloc[:,[column_index]]
		raw_data = data_column.iloc[current_stream_index][column_name]

		if (type_parser == 'general_parser'):
			data = self.general_parser(raw_data)
			return(data)

		if (type_parser == 'time_parser'):
                	data = self.time_parser(raw_data)
			return(data)

		if (type_parser == 'last_element_parser'):
                	data = self.last_element_parser(raw_data)
			return(data)
        '''
	#################################################################################################
	def streamrdd_to_df(self,srdd):


		df = srdd.toDF()


		## Pandas HACK
		pdf = self.deco.df_column_rename(df)

		# print(pdf)
		pdf_string = pdf.astype(str)
		pdf_string = pdf_string.applymap(self.deco.df_get_value)

		pdf_size = len(pdf.index)
		print("===================> Data Frame columns")
		print(pdf_string.columns)
		#print(pdf_string['seqno'])
        # print(pdf_size)

		for current_stream_index in range(0, pdf_size):

			# 1
			seqno = int(self.parser(pdf_string.ix[current_stream_index,'seqno']))
			print("seqno = ", seqno)

			#seqno = int(seqno.encode('ascii', 'ignore'))

			# 2
			peopleid = int(self.parser(pdf_string.ix[current_stream_index,'peopleid']))
			print("peopleid = ", peopleid)

			# 3
			# datetime = self.parser(pdf_string.ix[current_stream_index,'datetime'])


			# 4
			activityid = self.parser(pdf_string.ix[current_stream_index,'activityid'])
			print("activityid = ", activityid)


			# 5 - Do not need this
			# datasetfileid = self.parser(pdf_string.ix[current_stream_index,'datasetfileid'])

			# 6 - Do not need this
			# datasetid = self.parser(pdf_string.ix[current_stream_index,'datasetid'])

			# 7 - Do not need this
			heartrate = self.parser(pdf_string.ix[current_stream_index,'heartrate'])
			print("heartrate = ", heartrate)

			# 8
			imuankleacc16gxaxis = self.parser(pdf_string.ix[current_stream_index,'imuankleacc16gxaxis'])
			print("imuankleacc16gxaxis = ", imuankleacc16gxaxis)

			# 9
			imuankleacc16gyaxis = self.parser(pdf_string.ix[current_stream_index,'imuankleacc16gyaxis'])
			print("imuankleacc16gyaxis = ", imuankleacc16gyaxis)

			# 10
			imuankleacc16gzaxis = self.parser(pdf_string.ix[current_stream_index,'imuankleacc16gzaxis'])
			print("imuankleacc16gzaxis = ", imuankleacc16gzaxis)


			# 11
			imuankleacc6gxaxis = self.parser(pdf_string.ix[current_stream_index,'imuankleacc6gxaxis'])
			print("imuankleacc6gxaxis = ", imuankleacc6gxaxis)

			# 12
			imuankleacc6gyaxis = self.parser(pdf_string.ix[current_stream_index,'imuankleacc6gyaxis'])
			print("imuankleacc6gyaxis = ", imuankleacc6gyaxis)

			# 13
			#imuankleacc6gzaxis = self.parser(pdf_string.ix[current_stream_index,'imuankleacc6gzaxis'])
			#print("imuankleacc6gzaxis = ", imuankleacc6gzaxis

			# 14
			imuanklegyroxaxis = self.parser(pdf_string.ix[current_stream_index,'imuanklegyroxaxis'])
			print("imuanklegyroxaxis = ", imuanklegyroxaxis)

			# 15
			imuanklegyroyaxis = self.parser(pdf_string.ix[current_stream_index,'imuanklegyroyaxis'])

			print("imuanklegyroyaxis = ", imuanklegyroyaxis)

			# 16
			imuanklegyrozaxis = self.parser(pdf_string.ix[current_stream_index,'imuanklegyrozaxis'])
			print("imuanklegyrozaxis = ", imuanklegyrozaxis)

			# 17
			imuanklemagxaxis = self.parser(pdf_string.ix[current_stream_index,'imuanklemagxaxis'])
			print("imuanklemagxaxis = ", imuanklemagxaxis)

			# 18
			imuanklemagyaxis = self.parser(pdf_string.ix[current_stream_index,'imuanklemagyaxis'])
			print("imuanklemagyaxis = ", imuanklemagyaxis)

			# 19
			imuanklemagzaxis = self.parser(pdf_string.ix[current_stream_index,'imuanklemagzaxis'])
			print("imuanklemagzaxis = ", imuanklemagzaxis)

			# 20
			imuankleori1 = self.parser(pdf_string.ix[current_stream_index,'imuankleori1'])
			print("imuankleori1 = ", imuankleori1)

			# 21
			imuankleori2 = self.parser(pdf_string.ix[current_stream_index,'imuankleori2'])
			print("imuankleori2 = ", imuankleori2)

			# 22
			imuankleori3 = self.parser(pdf_string.ix[current_stream_index,'imuankleori3'])
			print("imuankleori3 = ", imuankleori3)

			# 23
			imuankleori4 = self.parser(pdf_string.ix[current_stream_index,'imuankleori4'])
			print("imuankleori4 = ", imuankleori4)

			# 24
			imuankletemp = self.parser(pdf_string.ix[current_stream_index,'imuankletemp'])
			print("imuankletemp = ", imuankletemp)

			# 25
			imuchestacc16gxaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestacc16gxaxis'])
			print("imuchestacc16gxaxis = ", imuchestacc16gxaxis)

			# 26
			imuchestacc16gyaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestacc16gyaxis'])
			print("imuchestacc16gyaxis = ", imuchestacc16gyaxis)

			# 27
			imuchestacc16gzaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestacc16gzaxis'])
			print("imuchestacc16gzaxis = ", imuchestacc16gzaxis)

			# 28
			imuchestacc6gxaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestacc6gxaxis'])
			print("imuchestacc6gxaxis = ", imuchestacc6gxaxis)

			# 29
			imuchestacc6gyaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestacc6gyaxis'])
			print("imuchestacc6gyaxis = ", imuchestacc6gyaxis)

			# 30
			imuchestacc6gzaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestacc6gzaxis'])
			print("imuchestacc6gzaxis = ", imuchestacc6gzaxis)

			# 31
			imuchestgyroxaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestgyroxaxis'])
			print("imuchestgyroxaxis = ", imuchestgyroxaxis)

			# 32
			imuchestgyroyaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestgyroyaxis'])
			print("imuchestgyroyaxis = ", imuchestgyroyaxis)

			# 33
			imuchestgyrozaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestgyrozaxis'])
			print("imuchestgyrozaxis = ", imuchestgyrozaxis)

			# 34
			imuchestmagxaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestmagxaxis'])
			print("imuchestmagxaxis = ", imuchestmagxaxis)

			# 35
			imuchestmagyaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestmagyaxis'])
			print("imuchestmagyaxis = ", imuchestmagyaxis)

			# 36
			imuchestmagzaxis = self.parser(pdf_string.ix[current_stream_index,'imuchestmagzaxis'])
			print("imuchestmagzaxis = ", imuchestmagzaxis)

			# 37
			imuchestori1 = self.parser(pdf_string.ix[current_stream_index,'imuchestori1'])
			print("imuchestori1 = ", imuchestori1)

			# 38
			imuchestori2 = self.parser(pdf_string.ix[current_stream_index,'imuchestori2'])
			print("imuchestori2 = ", imuchestori2)

			# 39
			imuchestori3 = self.parser(pdf_string.ix[current_stream_index,'imuchestori3'])
			print("imuchestori3 = ", imuchestori3)

			# 40
			imuchestori4 = self.parser(pdf_string.ix[current_stream_index,'imuchestori4'])
			print("imuchestori4 = ", imuchestori4)

			# 41
			imuchesttemp = self.parser(pdf_string.ix[current_stream_index,'imuchesttemp'])
			print("imuchesttemp = ", imuchesttemp)

			# 42
			imuhandacc16gxaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandacc16gxaxis'])
			print("imuhandacc16gxaxis = ", imuhandacc16gxaxis)

			# 43
			imuhandacc16gyaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandacc16gyaxis'])
			print("imuhandacc16gyaxis = ", imuhandacc16gyaxis)
			# 44
			imuhandacc16gzaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandacc16gzaxis'])
			print("imuhandacc16gzaxis = ", imuhandacc16gzaxis)
			# 45
			imuhandacc6gxaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandacc6gxaxis'])
			print("imuhandacc6gxaxis = ", imuhandacc6gxaxis)

			# 46
			imuhandacc6gyaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandacc6gyaxis'])
			print("imuhandacc6gyaxis = ", imuhandacc6gyaxis)

			# 47
			imuhandacc6gzaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandacc6gzaxis'])
			print("imuhandacc6gzaxis = ", imuhandacc6gzaxis)

			# 48
			imuhandgyroxaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandgyroxaxis'])
			print("imuhandgyroxaxis = ", imuhandgyroxaxis)

			# 49
			imuhandgyroyaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandgyroyaxis'])
			print("imuhandgyroyaxis = ", imuhandgyroyaxis)

			# 50
			imuhandgyrozaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandgyrozaxis'])
			print("imuhandgyrozaxis = ", imuhandgyrozaxis)

			# 51
			imuhandmagxaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandmagxaxis'])
			print("imuhandmagxaxis = ", imuhandmagxaxis)

			# 52
			imuhandmagyaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandmagyaxis'])
			print("imuhandmagyaxis = ", imuhandmagyaxis)

			# 53
			imuhandmagzaxis = self.parser(pdf_string.ix[current_stream_index,'imuhandmagzaxis'])
			print("imuhandmagzaxis = ", imuhandmagzaxis)

			# 54
			imuhandori1 = self.parser(pdf_string.ix[current_stream_index,'imuhandori1'])
			print("imuhandori1 = ", imuhandori1)

			# 55
			imuhandori2 = self.parser(pdf_string.ix[current_stream_index,'imuhandori2'])
			print("imuhandori2 = ", imuhandori2)

			# 56
			imuhandori3 = self.parser(pdf_string.ix[current_stream_index,'imuhandori3'])
			print("imuhandori3 = ", imuhandori3)

			# 57
			imuhandori4 = self.parser(pdf_string.ix[current_stream_index,'imuhandori4'])
			print("imuhandori4 = ", imuhandori4)

			# 58
			imuhandtemp = self.parser(pdf_string.ix[current_stream_index,'imuhandtemp'])
			print("imuhandtemp = ", imuhandtemp)

			# 59
			isactive = pdf_string.ix[current_stream_index,'isactive']
			print("isactive = ", isactive)

			# 60
			rowid = pdf_string.ix[current_stream_index,'rowid']
			print("rowid = ", rowid)

			# 61 - Not using this
			# sourceid = self.parser(pdf_string.ix[current_stream_index,'sourceid'])
			# print("sourceid = ", sourceid

			# changed the current_time to the below for the issue of date time in msec.
			# current_time = datetime.now()
			current_time = datetime.strptime(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3], '%Y-%m-%d %H:%M:%S.%f')

			actual_activity_id = activityid


			###############################################################################

			activity_id = float(activityid)
			print("Before Encoding activity_id = ", activity_id)
			activity_id = self.enc.Encode_Label(activity_id)
			print("After Encoding activity_id = ", activity_id)


			## Creation of Spark Session
			## Ganapathy - After we switch from Spark-Context to Streaming Context, if we try to use
			## Spark-SQL-Context to create the PySpark Dataframe, it is not able to switch the context
			## and then again revert back to Streaming-Context. That is the reason that I am
			## using creating and using Spark-Session. I do not understand fully what is happening under
			## the hood. Need your help to understand this on a Deep-Dive level

			spark = SparkSession.builder.config(conf=srdd.context.getConf()).getOrCreate()

			# Creation of Test Data-Pointt from the consumed message

			df_x = pd.DataFrame(np.arange(1,2).reshape(1,1))
			arr = sparse.coo_matrix(([float(activity_id),    \
				 float(heartrate),  	 \
								 float(imuankleacc16gxaxis), \
								 float(imuankleacc16gyaxis), \
								 float(imuankleacc16gzaxis), \
								 float(imuankleacc6gxaxis),  \
								 float(imuankleacc6gyaxis),  \
								 # float(imuankleacc6gzaxis),  \
								 float(imuanklegyroxaxis),   \
								 float(imuanklegyroyaxis),   \
								 float(imuanklegyrozaxis),   \
								 float(imuanklemagxaxis),    \
								 float(imuanklemagyaxis),    \
								 float(imuanklemagzaxis),    \
								 float(imuankleori1),        \
								 float(imuankleori2),        \
								 float(imuankleori3),        \
								 float(imuankleori4),        \
								 float(imuankletemp),        \
								 float(imuchestacc16gxaxis), \
								 float(imuchestacc16gyaxis), \
								 float(imuchestacc16gzaxis), \
								 float(imuchestacc6gxaxis),  \
								 float(imuchestacc6gyaxis),  \
								 float(imuchestacc6gzaxis),  \
								 float(imuchestgyroxaxis),   \
								 float(imuchestgyroyaxis),   \
								 float(imuchestgyrozaxis),   \
								 float(imuchestmagxaxis),    \
								 float(imuchestmagyaxis),    \
								 float(imuchestmagzaxis),    \
								 float(imuchestori1),        \
								 float(imuchestori2),        \
								 float(imuchestori3),        \
								 float(imuchestori4),        \
								 float(imuchesttemp),        \
								 float(imuhandacc16gxaxis),  \
								 float(imuhandacc16gyaxis),  \
								 float(imuhandacc16gzaxis),  \
								 float(imuhandacc6gxaxis),   \
								 float(imuhandacc6gyaxis),   \
								 float(imuhandacc6gzaxis),   \
								 float(imuhandgyroxaxis),    \
								 float(imuhandgyroyaxis),    \
								 float(imuhandgyrozaxis),    \
								 float(imuhandmagxaxis),     \
								 float(imuhandmagyaxis),     \
								 float(imuhandmagzaxis),     \
								 float(imuhandori1),         \
								 float(imuhandori2),         \
								 float(imuhandori3),         \
								 float(imuhandori4),         \
								 float(imuhandtemp)]),
			 shape=(1,1))

			df_x['value'] = arr.toarray().tolist()
			df_1 = df_x.iloc[:,[1]]
			# print df_1


			test_point = spark.createDataFrame(df_1)
			test_point_rdd = test_point.rdd

			# print "&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&"
		  	# print(test_point_rdd.take(1))


			## Fundamental Data-Structure in Spark MLLib is a LabeledPoint comprising of a label
			## and a dense vector of features.
			test_df = LabelPoint_Preparation(test_point_rdd)
			print("&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&")
			print(test_df.take(1))
			print('&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&&')
			###########################################################################


			## Generating the Prediction Results
			predictions = self.Activity_Prediction_Results(test_df,peopleid)
			pred = predictions.take(1)
			predicted_activity = pred[0]
			##################################################################################

			###################################################################################
			## Calculation of Prediction Sub-Interval Accuracy
			if(predicted_activity == activity_id):
				self.CORRECT_PREDICTED_ACTIVITY_COUNTER += 1
				self.PREDICTION_ACCURACY = \
				float((self.CORRECT_PREDICTED_ACTIVITY_COUNTER/(self.Row_Index+1)) * 100)
			##########################################################################################

			## Insertion to the Activity Prediction Table

			# self.data_access_layer.insert_activity_prediction(people_id, current_time,   \
			#	seqno,activity_id, predicted_activity, self.PREDICTION_ACCURACY, heartbeat)

			self.conn.insert_activity_prediction(int(peopleid), current_time,   \
				int(seqno) ,activity_id, predicted_activity, self.PREDICTION_ACCURACY, heartrate)

			actual_activity_id = activity_id
			activity_id = predicted_activity ## Work with Predicted Activity from here on

################################################################################################################
			## Numenta Code
			self.Numenta_Operations(current_time, peopleid, \
                                                      actual_activity_id, activity_id, current_time, heartrate, seqno)

			self.Row_Index += 1

################################################################################################################

	def process_rdd(self,rdd):
        	self.streamrdd_to_df(rdd)
		## Flush-out RDD after usage


	def empty_rdd(self):
        	print("############# The current RDD is empty. Wait for the next complete RDD #####################")

	def trigger_stream(self):

		# modify with Heroku kafka consumer part....

		'''
		self.kafkaStream = KafkaUtils.createDirectStream(self.ssc, ["test"], \
					{"metadata.broker.list": self.hv.KAFKA_SCHEDULER })
		'''
		
		print("=====> Kafka Brokers obtained are =====>")
		print(get_kafka_brokers())
		kafka_brokers = ','.join(map(str,get_kafka_brokers()))
		print('======> Converted String ======>')
		print(kafka_brokers)
		print('Kakfka Topic ======>')
		print(self.hv.KAFKA_TOPIC_QUEUE)

		'''
		self.kafkaStream = KafkaUtils.createDirectStream(self.ssc, [self.hv.KAFKA_TOPIC_QUEUE], \
														 {"metadata.broker.list": kafka_brokers})


		self.kafkaStream = KafkaUtils.createDirectStream(self.ssc, [self.hv.KAFKA_TOPIC_QUEUE], \
												 {"metadata.broker.list": kafka_brokers})
		'''

		zkQuorum, topic = get_zk_list(), self.hv.KAFKA_TOPIC_QUEUE
		print(zkQuorum)
		zk_url = ','.join(map(str, get_zk_list()))

		self.kafkaStream = KafkaUtils.createStream(self.ssc, zk_url, "spark-streaming-consumer", {topic:1})

		print('printing K ======>')

		print(type(self.kafkaStream))
		print(self.kafkaStream)


		self.raw = self.kafkaStream.flatMap(lambda kafkaS: [kafkaS])
		self.clean = self.raw.map(lambda xs: xs[1].split(","))
		self.clean.foreachRDD(lambda rdd: self.empty_rdd() if rdd.count() == 0 else self.process_rdd(rdd))

		self.ssc.start()             # Start the computation
		self.ssc.awaitTermination()  # Wait for the computation to terminate


if __name__ == '__main__':
	c = Consumer()
	c.trigger_stream()


########################### EOF ################################################################
