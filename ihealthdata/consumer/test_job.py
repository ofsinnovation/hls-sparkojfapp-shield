## Imports

from pyspark import SparkConf, SparkContext

## CONSTANTS

APP_NAME = "My Spark Application"

##OTHER FUNCTIONS/CLASSES

## Main functionality

def main(sc):
    rdd = sc.parallelize(range(1000), 10)

    print(rdd.mean())

if __name__ == "__main__":
    # Configure OPTIONS
    conf = SparkConf().setAppName(APP_NAME)
    # conf = conf.setMaster("local[*]")
    #in cluster this will be like
    #"spark://ec2-0-17-03-078.compute-#1.amazonaws.com:7077"
    sc   = SparkContext(conf=conf)
    # Execute Main functionality
    main(sc)