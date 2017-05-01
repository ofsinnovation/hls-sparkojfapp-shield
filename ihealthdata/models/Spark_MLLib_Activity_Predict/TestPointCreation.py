
from ihealthdata.utils.Encoder import * 

class TestPointCreation(object):

	def __init__(self):
		self.enc = Encoder()

	def LabelPoint_Preparation(l):
                l = [float(x) for x in l]
                return LabeledPoint(l[0],l[1:])

	def test_data_point_creation(self,activity_id, heartbeat, imuhandtemp):

                activity_id = float(activity_id)

                activity_id = self.enc.Encode_Label(activity_id)
                spark = getSparkSessionInstance(srdd.context.getConf())
       
                df_x = pd.DataFrame(np.arange(1,2).reshape(1,1))
                arr = sparse.coo_matrix(([float(activity_id),float(heartbeat),float(imuhandtemp)]), shape=(1,1))
                df_x['value'] = arr.toarray().tolist()
                df_1 = df_x.iloc[:,[1]]
                test_point = spark.createDataFrame(df_1)

                test_df = test_point.rdd.map(lambda l: LabelPoint_Preparation(l[0]))
                test_df.take(1)

                return(test_df)
