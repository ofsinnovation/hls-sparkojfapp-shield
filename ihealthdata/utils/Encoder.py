
import SwitchCase
from ihealthdata.helper.HelperVariable import *

class Encoder(object):

	def __init__(self):

        	self.hv = HelperVariable()
	
	def Encode_Label(self,raw_activity):

        	self.encoded_activity = self.hv.ACTIVITY_ENCODER_DICTIONARY_INITIALIZER[raw_activity]
		return(self.encoded_activity)


if __name__ == '__main__':
	hv = HelperVariable()
	print(hv.ACTIVITY_ENCODER_DICTIONARY_INITIALIZER[24.0])

	
        encoder = Encoder()
	encoded_activity = encoder.Encode_Label(24.0)
	print(encoded_activity)


#######################################################################################################
