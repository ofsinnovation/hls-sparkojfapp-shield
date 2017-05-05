from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.streaming import StreamingContext
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel
from pyspark.mllib.regression import LabeledPoint
from pyspark.sql import SparkSession
from pyspark.streaming.kafka import KafkaUtils
import numpy as np
import pip


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
    test_rdd = sc.parallelize(np.array([1,2,3,4]))
    test_rdd.collect()
    print('Rdd Collected')


def install_dependencies(sc):
    int_rdd = sc.parallelize([1, 2, 3, 4])
    int_rdd.map(lambda x: install_and_import(x))
    int_rdd.collect()


if __name__ == '__main__':
    sc = SparkContext()
    sqlContext = SQLContext(sc)
    install_dependencies(sc)
    test_job(sc)
