
import SwitchCase

class Encoder(object):
	
	def Encode_Label(self,raw_activity):

        	self.encoded_activity = 0.0

		hv.TOTAL_DICTIONARY[peopleid]

		'''
        	# encode_label From Scala Code
        	# case 1 => 0.0
        	# case 2 => 1.0
        	# case 3 => 2.0
        	# case 4 => 3.0
        	# case 5 => 4.0
        	# case 6 => 5.0
        	# case 7 => 6.0
        	# case 9 => 7.0
        	# case 10 => 8.0
        	# case 11 => 9.0
        	# case 12 => 10.0
        	# case 13 => 11.0
        	# case 16 => 12.0
        	# case 17 => 13.0
        	# case 18 => 14.0
        	# case 19 => 15.0
        	# case 20 => 16.0
        	# case 24 => 17.0
        	# case 0 => 18.0
           
	        for case in SwitchCase(raw_activity):
       
                	if case(0.0):
                        	self.encoded_activity = 18.0
                        	break

                	if case(1.0):
                        	self.encoded_activity = 0.0
                        	break

                	if case(2.0):
                        	self.encoded_activity = 1.0
                        	break

                	if case(3.0):
                        	self.encoded_activity = 2.0
                        	break

                	if case(4.0):
                        	self.encoded_activity = 3.0
                        	break

                	if case(5.0):
                        	self.encoded_activity = 4.0
                        	break

                	if case(6.0):
                        	self.encoded_activity = 5.0
                                break                               
		
                	if case(7.0):
                        	self.encoded_activity = 6.0
                        	break

                	if case(9.0):
                        	self.encoded_activity = 7.0
                        	break

                	if case(10.0):
                        	self.encoded_activity = 8.0
                        	break

                	if case(11.0):
                        	self.encoded_activity = 9.0
                        	break

                	if case(12.0):
                        	self.encoded_activity = 10.0
                        	break

                	if case(13.0):
                        	self.encoded_activity = 11.0
                        	break

                	if case(16.0):
                        	self.encoded_activity = 12.0
                        	break

                	if case(17.0):
                        	self.encoded_activity = 13.0
                        	break

                	if case(18.0):
                        	self.encoded_activity = 14.0
                        	break

                	if case(19.0):
                        	self.encoded_activity = 15.0
                        	break

                	if case(20.0):
                        	self.encoded_activity = 16.0
                        	break

                	if case(24.0):
                        	self.encoded_activity = 17.0
                        	break
		'''
		return self.encoded_activity


if __name__ == '__main__':
        hv = HelperVariable()
        # c = Consumer()
        # c.trigger_stream()
        encoder = Encoder()
	encoded_activity = encoder.Encode_Label(1)
	print(encoded_activity)

#######################################################################################################
