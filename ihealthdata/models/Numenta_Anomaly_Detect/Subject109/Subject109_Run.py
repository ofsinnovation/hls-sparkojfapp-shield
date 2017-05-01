#!/usr/bin/env python
# ----------------------------------------------------------------------
# Numenta Platform for Intelligent Computing (NuPIC)
# Copyright (C) 2013, Numenta, Inc.  Unless you have an agreement
# with Numenta, Inc., for a separate license for this software code, the
# following terms and conditions apply:
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero Public License version 3 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Affero Public License for more details.
#
# You should have received a copy of the GNU Affero Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#
# http://numenta.org/licenses/
# ----------------------------------------------------------------------
"""
Groups together code used for creating a NuPIC model and dealing with IO.
(This is a component of the One Hot Gym Anomaly Tutorial.)
"""
import importlib
import sys
import csv
#import datetime
from datetime import datetime
import pandas as pd
# from cassandra.cluster import Cluster
from nupic.frameworks.opf.modelfactory import ModelFactory
import time
import nupic_anomaly_output
import yaml

# Activity Encoder
#with open("config/activity_config.yml", 'r') as ymlfile:
#    cfg = yaml.load(ymlfile)

DESCRIPTION = (
  "Starts a NuPIC model from the model params returned by the swarm\n"
  "and pushes each line of input from the gym into the model. Results\n"
  "are written to an output file (default) or plotted dynamically if\n"
  "the --plot option is specified.\n"
)

DATA_FILE = "heart-beat"
DATA_DIR = "." # "/home/ksaha/Data/Data_Run"
MODEL_PARAMS_DIR = "./model_params"
INPUT_DATA = "anomaly-data.csv" # "%s/%s.csv" % (DATA_DIR, DATA_FILE.replace(" ", "_"))
# '7/2/10 0:00'
DATE_FORMAT = "%Y-%m-%d %H:%M:%S.%f"
#DATE_FORMAT = "%m/%d/%y %H:%M"
TOTAL_RECORDS = 2543  # -1 record from the total size

def createModel(modelParams):
  """
  Given a model params dictionary, create a CLA Model. Automatically enables
  inference for kw_energy_consumption.
  :param modelParams: Model params dict
  :return: OPF Model object
  """
  model = ModelFactory.create(modelParams)
  model.enableInference({"predictedField": "heartbeat"})
  return model


def getModelParamsFromName():
  """
  Given a gym name, assumes a matching model params python module exists within
  the model_params directory and attempts to import it.
  :return: OPF Model params dictionary

  """
  importName = "model_params.%s_model_params" % (
    DATA_FILE.replace(" ", "_").replace("-", "_")
  )

#  print "Importing model params from %s" % importName

  try:
    importedModelParams = importlib.import_module(importName).MODEL_PARAMS
  except ImportError:
    raise Exception("No model params exist for '%s'. Run swarm first!"
                    % DATA_FILE)
  return importedModelParams


def runIoThroughNupic(model):
  """
  Handles looping over the input data and passing each row into the given model
  object, as well as extracting the result object and passing it into an output
  handler.
  :param inputData: file path to input data CSV
  :param model: OPF Model object
  """
  parsing_error_counter = 0
  
  output = nupic_anomaly_output.NuPICFileOutput(DATA_FILE)

  cluster = Cluster(['10.10.40.138'])
  session = cluster.connect('hotgym')  # key-space = hotgym
  print ("Connected!")

  num_records_index = 0
#  total_records = 2543  # -1 record from the total size 
  patient_id = '109'
  
  # Master Data Source
  df = pd.read_csv(INPUT_DATA)
  
  time_diff = datetime.strptime(str(datetime.utcnow()), DATE_FORMAT)

  while(num_records_index < TOTAL_RECORDS):

        print "Processing record = %i ..." % num_records_index
        val_time_stamp = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
	        
        val_heartbeat = str(df.iloc[num_records_index,1])

	val_activity = cfg[str(df.iloc[num_records_index,2])]
     
        val_V13 = str(df.iloc[num_records_index,12])

        CQL_Insert_String = "INSERT INTO data_input_11_29_4 (patient_id,time_stamp, heartbeat, activity, V13_value)" \
			    + "VALUES ('" + patient_id + "','"+ val_time_stamp + "','" + \
                            val_heartbeat + "','" + val_activity +  "','" +  val_V13 +"');"

        session.execute(CQL_Insert_String)
	        
        # Add the record from the actual data source to the Cassandra input table
        ## Read back the latest row added from Cassandra input table
        CQLString = "SELECT * FROM data_input_11_29_4  LIMIT 1;" 
        rows = session.execute(CQLString)
        for user_row in rows:
                data_df = pd.DataFrame({'col_1' : [user_row.time_stamp], \
					'col_2' : [user_row.heartbeat],
					'col_3' : [user_row.activity],
					'col_4' : [user_row.v13_value]})

        datetime_str = str(data_df.iloc[0,0])
        if len(datetime_str) == 19:     # Taking care of the parsing error when no millisecs are present in timestamp
                parsing_error_counter += 1
                datetime_str = datetime_str +".001"
        
	timestamp = datetime.strptime(str(datetime_str), DATE_FORMAT) 
		
        heartbeat = int(data_df.iloc[0,1])
        activity = data_df.iloc[0,2]
        V13_value = float(data_df.iloc[0,3])

        ## Code for mapping activity to be written
             
        result = model.run({
                            "timestamp": timestamp,
                            "heartbeat": heartbeat,
			    "V13": V13_value
                           })
         
        prediction = result.inferences["multiStepBestPredictions"][1]
        anomalyScore = result.inferences["anomalyScore"]
        ## Write this anomaly score to the Cassandra output table
         
        anomalyLikelihood = output.write(timestamp, heartbeat, prediction, anomalyScore)

	time_diff = timestamp - time_diff

        output_timestamp = str(timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        print output_timestamp, heartbeat, int(prediction), anomalyScore, anomalyLikelihood 
         
        CQL_Output_String = "INSERT INTO data_output_11_29_4 (patient_id,timestamp,heartbeat, \
                             prediction,anomalyScore,anomalyLikelihood,activity) " + "VALUES ('" + \
                             patient_id + "','"+ output_timestamp + "','" + str(heartbeat) + \
			     "','" + str(int(prediction)) + "','" + str(anomalyScore) + "','" + \
			     str(anomalyLikelihood) + "','" + str(activity) + "');"

        session.execute(CQL_Output_String)

	# Need to get the Anomaly of Anomalies concept sorted out
        #if ((anomalyScore > 0.8) and (anomalyLikelihood > 0.8) and (anomalyScore > anomalyLikelihood)):
	if (anomalyScore > 0.8):		

		CQL_Anomaly_String = "INSERT INTO data_anomaly_11_29_4 (patient_id,timestamp,heartbeat, \
        	                      prediction,anomalyScore,anomalyLikelihood,activity,record_num) " + \
                                     "VALUES ('" + patient_id + "','"+ output_timestamp + "','" + str(heartbeat) + \
                        	      "','" + str(int(prediction)) + "','" + str(anomalyScore) + "','" + \
                             str(anomalyLikelihood) + "','" + str(activity) + "','" + str(num_records_index+2) + "');"

        	session.execute(CQL_Anomaly_String)
	
	# Just for statistics gathering. Will remove this table	
	CQL_time_diff_String = "INSERT INTO data_time_diff_11_29_4 (patient_id,timestamp,time_diff) " + \
			        "VALUES ('" + patient_id + "','"+ output_timestamp + "','" + str(time_diff) + "');"


        session.execute(CQL_time_diff_String)
               
	time_diff = timestamp  
        # To simulate real-time case we add delay here

  #      print("Waiting for the next record. Delay is 1 sec")

        time.sleep(.063)
	
        num_records_index += 1

  print("parsing_error_counter value+++++++++++++++++++++++++++++")
  print parsing_error_counter


def runModel():
  """
  Assumes the gynName corresponds to both a like-named model_params file in the
  model_params directory, and that the data exists in a like-named CSV file in
  the current directory.
  """
  print "Creating model from %s..." % DATA_FILE
  model = createModel(getModelParamsFromName())
  runIoThroughNupic(model)

if __name__ == "__main__":
  print DESCRIPTION
  start_time = time.time()
  runModel()
  print("--Total Time Taken = %s seconds ------------------------" % (time.time() - start_time))
  print("-- Per row avg -- = % d milliseconds -------------------" % ((time.time() - start_time)/TOTAL_RECORDS*1000))

##############################################################################################

###########  Fresh Code Starts here ########################################################

def create_Numenta_Model_109():

        importName = "model_params.%s_model_params" % (DATA_FILE.replace(" ", "_").replace("-", "_"))
#        print "Importing model params from %s" % importName

        importedModelParams = importlib.import_module(importName).MODEL_PARAMS
#        print importedModelParams

        model = ModelFactory.create(importedModelParams)
        model.enableInference({"predictedField": "heartbeat"})
#        print model
        return model

def model_Result_109(raw_timestamp,heartbeat,model):

#	model = createModel(getModelParamsFromName())
  
  	# Need to figure out Anomaly Likelihood code
	# output = nupic_anomaly_output.NuPICFileOutput(DATA_FILE)

	datetime_str = str(datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
        # datetime_str = str(raw_timestamp)

        if len(datetime_str) == 19:     # Taking care of the parsing error when no millisecs are present in timestamp
                datetime_str = datetime_str +".001"

        timestamp = datetime.strptime(str(datetime_str), DATE_FORMAT)

        #    print timestamp

        heartbeat = int(heartbeat)
        
        result = model.run({
                            "timestamp": timestamp,
                            "heartbeat": heartbeat
                           })

        prediction = result.inferences["multiStepBestPredictions"][1]
        anomalyScore = result.inferences["anomalyScore"]

	# anomalyLikelihood = Need to figure this out
	# anomalyLikelihood = output.write(timestamp, heartbeat, prediction, anomalyScore)

        output_timestamp = str(timestamp.strftime('%Y-%m-%d %H:%M:%S.%f')[:-3])
#        print output_timestamp, heartbeat, int(prediction), anomalyScore #, anomalyLikelihood
        
	return(output_timestamp,heartbeat,int(prediction),anomalyScore)

        # Need to get the Anomaly of Anomalies concept sorted out
        #if ((anomalyScore > 0.8) and (anomalyLikelihood > 0.8) and (anomalyScore > anomalyLikelihood)):
        
	# To simulate real-time case we add delay here
	# print("Waiting for the next record. Delay is 1 sec")
	# time.sleep(.063)

###############################################################################################################
