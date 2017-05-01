
## This is the Kafka-Producer Class
import ihealthdata.utils.loggerutils as logger
from ihealthdata.helper.HelperVariable import *
# from kafka import KafkaProducer
import kafka_helper


class Kafka_Consumer(object):

	def __init__(self):
	
		# Instantiating HelperVariable
		self.hv = HelperVariable()

        # Setting up Kafka-Brokers
		# self.producer = KafkaProducer(bootstrap_servers = '10.0.0.15:31000,10.0.0.13:31000,10.0.0.14:31000,10.0.0.16:31000,10.0.0.16:31001')

		# self.producer = KafkaProducer(bootstrap_servers = self.hv.KAFKA_SCHEDULER)

		self.consumer = kafka_helper.get_kafka_consumer(topic=self.hv.KAFKA_TOPIC_QUEUE)
		print(self.consumer)

	def consumer(self, data_point):
		consumer = kafka_helper.get_kafka_consumer(topic=self.hv.KAFKA_TOPIC_QUEUE)
		for message in consumer:
			print(message)

	'''
	def produce(self, data_point):

		d = {"peopleid":data_point.iloc[0]['peopleid'], "datetime":data_point.iloc[0]['datetime'] ,\
				  "heartrate":data_point.iloc[0]['heartrate'], "activityid":data_point.iloc[0]['activityid'], \
				 "seqno":data_point.iloc[0]['seqno'], "imuhandtemp":data_point.iloc[0]['imuhandtemp']}




		d = {"seqno": data_point.iloc[0]['seqno'], "peopleid": data_point.iloc[0]['peopleid'], \
			 "datetime": data_point.iloc[0]['datetime'], \
			 "activityid": data_point.iloc[0]['activityid'], \
			 "datasetfileid": data_point.iloc[0]['datasetfileid'], \
			 "datasetid": data_point.iloc[0]['datasetid'], \
			 "heartrate": data_point.iloc[0]['heartrate'], \
			 "imuankleacc16gxaxis": data_point.iloc[0]['imuankleacc16gxaxis'], \
			 "imuankleacc16gyaxis": data_point.iloc[0]['imuankleacc16gyaxis'], \
			 "imuankleacc16gzaxis": data_point.iloc[0]['imuankleacc16gzaxis'], \
			 "imuankleacc6gxaxis": data_point.iloc[0]['imuankleacc6gxaxis'], \
			 "imuankleacc6gyaxis": data_point.iloc[0]['imuankleacc6gyaxis'], \
			 "imuankleacc6gzaxis": data_point.iloc[0]['imuankleacc6gzaxis'], \
			 "imuanklegyroxaxis": data_point.iloc[0]['imuanklegyroxaxis'], \
			 "imuanklegyroyaxis": data_point.iloc[0]['imuanklegyroyaxis'], \
			 "imuanklegyrozaxis": data_point.iloc[0]['imuanklegyrozaxis'], \
			 "imuanklemagxaxis": data_point.iloc[0]['imuanklemagxaxis'], \
			 "imuanklemagyaxis": data_point.iloc[0]['imuanklemagyaxis'], \
			 "imuanklemagzaxis": data_point.iloc[0]['imuanklemagzaxis'], \
			 "imuankleori1": data_point.iloc[0]['imuankleori1'], \
			 "imuankleori2": data_point.iloc[0]['imuankleori2'], \
			 "imuankleori3": data_point.iloc[0]['imuankleori3'], \
			 "imuankleori4": data_point.iloc[0]['imuankleori4'], \
			 "imuankletemp": data_point.iloc[0]['imuankletemp'], \
			 "imuchestacc16gxaxis": data_point.iloc[0]['imuchestacc16gxaxis'], \
			 "imuchestacc16gyaxis": data_point.iloc[0]['imuchestacc16gyaxis'], \
			 "imuchestacc16gzaxis": data_point.iloc[0]['imuchestacc16gzaxis'], \
			 "imuchestacc6gxaxis": data_point.iloc[0]['imuchestacc6gxaxis'], \
			 "imuchestacc6gyaxis": data_point.iloc[0]['imuchestacc6gyaxis'], \
			 "imuchestacc6gzaxis": data_point.iloc[0]['imuchestacc6gzaxis'], \
			 "imuchestgyroxaxis": data_point.iloc[0]['imuchestgyroxaxis'], \
			 "imuchestgyroyaxis": data_point.iloc[0]['imuchestgyroyaxis'], \
			 "imuchestgyrozaxis": data_point.iloc[0]['imuchestgyrozaxis'], \
			 "imuchestmagxaxis": data_point.iloc[0]['imuchestmagxaxis'], \
			 "imuchestmagyaxis": data_point.iloc[0]['imuchestmagyaxis'], \
			 "imuchestmagzaxis": data_point.iloc[0]['imuchestmagzaxis'], \
			 "imuchestori1": data_point.iloc[0]['imuchestori1'], \
			 "imuchestori2": data_point.iloc[0]['imuchestori2'], \
			 "imuchestori3": data_point.iloc[0]['imuchestori3'], \
			 "imuchestori4": data_point.iloc[0]['imuchestori4'], \
			 "imuchesttemp": data_point.iloc[0]['imuchesttemp'], \
			 "imuhandacc16gxaxis": data_point.iloc[0]['imuhandacc16gxaxis'], \
			 "imuhandacc16gyaxis": data_point.iloc[0]['imuhandacc16gyaxis'], \
			 "imuhandacc16gzaxis": data_point.iloc[0]['imuhandacc16gzaxis'], \
			 "imuhandacc6gxaxis": data_point.iloc[0]['imuhandacc6gxaxis'], \
			 "imuhandacc6gyaxis": data_point.iloc[0]['imuhandacc6gyaxis'], \
			 "imuhandacc6gzaxis": data_point.iloc[0]['imuhandacc6gzaxis'], \
			 "imuhandgyroxaxis": data_point.iloc[0]['imuhandgyroxaxis'], \
			 "imuhandgyroyaxis": data_point.iloc[0]['imuhandgyroyaxis'], \
			 "imuhandgyrozaxis": data_point.iloc[0]['imuhandgyrozaxis'], \
			 "imuhandmagxaxis": data_point.iloc[0]['imuhandmagxaxis'], \
			 "imuhandmagyaxis": data_point.iloc[0]['imuhandmagyaxis'], \
			 "imuhandmagzaxis": data_point.iloc[0]['imuhandmagzaxis'], \
			 "imuhandori1": data_point.iloc[0]['imuhandori1'], \
			 "imuhandori2": data_point.iloc[0]['imuhandori2'], \
			 "imuhandori3": data_point.iloc[0]['imuhandori3'], \
			 "imuhandori4": data_point.iloc[0]['imuhandori4'], \
			 "imuhandtemp": data_point.iloc[0]['imuhandtemp'], "\
					isactive": data_point.iloc[0]['isactive'], \
			 "rowid": data_point.iloc[0]['rowid'], \
			 "sourceid": data_point.iloc[0]['sourceid']}


		strMessage = str(d)
		print(strMessage)
		print("Sending Message")
		print(self.hv.KAFKA_TOPIC_QUEUE)
		self.producer.send(self.hv.KAFKA_TOPIC_QUEUE, strMessage)
		print("producer success")

		return
		'''

'''
if __name__ == '__main__':
	kp = Kafka_Producer()
	d = {"key1": 1, "value": 2}
	query = kp.produce(d)
'''
############################ EOF #############################################################################
