from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
import numpy as np
import pip
import sys

from ihealthdata.helper.HelperVariable import *
# from ihealthdata.models.Spark_MLLib_Activity_Predict.TestPointCreation import TestPointCreation
from ihealthdata.utils.SwitchCase import SwitchCase
from ihealthdata.consumer.ParserDecorator import ParserDecorator
from ihealthdata.persistence.pgsql_connector import PostgresConnector

sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject101')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject102')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject103')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject104')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject105')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject106')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject107')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject108')
sys.path.insert(0, '/app/ihealthdata/models/Numenta_Anomaly_Detect/Subject109')

ACTIVITY_PREDICTION_MODEL_DICTIONARY = {101: '', 102: '', 103: '', 104: '', 105: '', 106: '', 107: '', 108: '',
                                        109: ''}

global activity_detection_model_101



def LabelPoint_Creation(l):
    l = [float(x) for x in l]
    return LabeledPoint(l[0], l[1:])


def LabelPoint_Preparation(data):
    test_df = data.map(lambda l_data: LabelPoint_Creation(l_data[0]))
    return (test_df)


def install_and_import(x):
    pip.main(['install', 'numpy=1.11.2'])
    pip.main(['install', 'configparser==3.5.0'])
    pip.main(['install', 'nupic==0.5.7'])
    pip.main(['install', 'kafka-python==1.3.1'])
    pip.main(['install', 'sqlalchemy'])
    pip.main(['install', 'psycopg2'])
    pip.main(['install', 'cryptography'])
    pip.main(['install', 'pandas==0.19.1'])
    pip.main(['install', 'scipy==0.18.1'])
    return x


def test_job(sc):
    test_rdd = sc.parallelize(np.array([1, 2, 3, 4]))
    test_rdd.collect()
    print('Rdd Collected')


def install_dependencies(sc):
    int_rdd = sc.parallelize([1, 2, 3, 4])
    int_rdd.map(lambda x: install_and_import(x))
    int_rdd.collect()


def activity_prediction_model_preparation(peopleid):
    pandas_df = conn.activity_historical_data_access(peopleid)
    print("~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~  " + str(peopleid))
    spark_df = sqlContext.createDataFrame(pandas_df)
    ACTIVITY_ENCODER_DICTIONARY = hv.ACTIVITY_ENCODER_DICTIONARY_INITIALIZER
    df_dataset = spark_df.rdd.map(
        lambda row: LabeledPoint(ACTIVITY_ENCODER_DICTIONARY[row.activityid],
                                 [float(row.heartrate), float(row.imuankleacc16gxaxis), float(row.imuankleacc16gyaxis),
                                  float(row.imuankleacc16gzaxis), float(row.imuankleacc6gxaxis),
                                  float(row.imuankleacc6gyaxis), float(row.imuanklegyroxaxis),
                                  float(row.imuanklegyroyaxis), float(row.imuanklegyrozaxis),
                                  float(row.imuanklemagxaxis), float(row.imuanklemagyaxis),
                                  float(row.imuanklemagzaxis), float(row.imuankleori1),
                                  float(row.imuankleori2),float(row.imuankleori3),
                                  float(row.imuankleori4),
                                  float(row.imuankletemp),
                                  float(row.imuchestacc16gxaxis),
                                  float(row.imuchestacc16gyaxis),
                                  float(row.imuchestacc16gzaxis),
                                  float(row.imuchestacc6gxaxis),
                                  float(row.imuchestacc6gyaxis),
                                  float(row.imuchestacc6gzaxis),
                                  float(row.imuchestgyroxaxis),
                                  float(row.imuchestgyroyaxis),
                                  float(row.imuchestgyrozaxis),
                                  float(row.imuchestmagxaxis),
                                  float(row.imuchestmagyaxis),
                                  float(row.imuchestmagzaxis),
                                  float(row.imuchestori1),
                                  float(row.imuchestori2),
                                  float(row.imuchestori3),
                                  float(row.imuchestori4),
                                  float(row.imuchesttemp),
                                  float(row.imuhandacc16gxaxis),
                                  float(row.imuhandacc16gyaxis),
                                  float(row.imuhandacc16gzaxis),
                                  float(row.imuhandacc6gxaxis),
                                  float(row.imuhandacc6gyaxis),
                                  float(row.imuhandacc6gzaxis),
                                  float(row.imuhandgyroxaxis),
                                  float(row.imuhandgyroyaxis),
                                  float(row.imuhandgyrozaxis),
                                  float(row.imuhandmagxaxis),
                                  float(row.imuhandmagyaxis),
                                  float(row.imuhandmagzaxis),
                                  float(row.imuhandori1),
                                  float(row.imuhandori2),
                                  float(row.imuhandori3),
                                  float(row.imuhandori4),
                                  float(row.imuhandtemp)]))
    trainingData, testData = df_dataset.randomSplit([1.0, 0.0])
    num_classes = hv.NUM_CLASSES
    impurity = hv.IMPURITY
    max_depth = hv.MAX_DEPTH
    max_bins = hv.MAX_BINS

    if (peopleid == 101):
        activity_detection_model_101 = DecisionTree.trainClassifier(trainingData, numClasses=num_classes, categoricalFeaturesInfo={},impurity=impurity, maxDepth=max_depth, maxBins=max_bins)

    if (peopleid == 102):
        activity_detection_model_102 = DecisionTree.trainClassifier(trainingData, numClasses=num_classes, categoricalFeaturesInfo={},impurity=impurity, maxDepth=max_depth, maxBins=max_bins)

    if (peopleid == 103):
        activity_detection_model_103 = DecisionTree.trainClassifier(trainingData, numClasses=num_classes, categoricalFeaturesInfo={},impurity=impurity, maxDepth=max_depth, maxBins=max_bins)

    if (peopleid == 104):
        activity_detection_model_104 = DecisionTree.trainClassifier(trainingData, numClasses=num_classes, categoricalFeaturesInfo={},impurity=impurity, maxDepth=max_depth, maxBins=max_bins)

    if (peopleid == 105):
        activity_detection_model_105 = DecisionTree.trainClassifier(trainingData, numClasses=num_classes, categoricalFeaturesInfo={},impurity=impurity, maxDepth=max_depth, maxBins=max_bins)

    if (peopleid == 106):
        activity_detection_model_106 = DecisionTree.trainClassifier(trainingData, numClasses=num_classes, categoricalFeaturesInfo={},impurity=impurity, maxDepth=max_depth, maxBins=max_bins)

    if (peopleid == 107):
        activity_detection_model_107 = DecisionTree.trainClassifier(trainingData, numClasses=num_classes, categoricalFeaturesInfo={},impurity=impurity, maxDepth=max_depth, maxBins=max_bins)

    if (peopleid == 108):
        activity_detection_model_108 = DecisionTree.trainClassifier(trainingData, numClasses=num_classes, categoricalFeaturesInfo={},impurity=impurity, maxDepth=max_depth, maxBins=max_bins)

    if (peopleid == 109):
        activity_detection_model_109 = DecisionTree.trainClassifier(trainingData, numClasses=num_classes, categoricalFeaturesInfo={},impurity=impurity, maxDepth=max_depth, maxBins=max_bins)

    print("======> Models successfully Created")

def activity_prediction_model_creation():
    map(activity_prediction_model_preparation, hv.SUBJECT_LIST_ID)


if __name__ == '__main__':
    hv = HelperVariable()
    deco = ParserDecorator()
    conn = PostgresConnector()
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    install_dependencies(sc)
    #sc.addPyFile("dependencies.zip")
    activity_prediction_model_creation()
