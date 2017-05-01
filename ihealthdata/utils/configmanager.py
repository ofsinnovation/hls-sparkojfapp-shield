import configparser as configParser
import os


class ConfigManager:

    def __init__(self):
	self.config_parser = configParser.ConfigParser()
        file_name = os.path.join(os.getcwd(), 'config.ini')
	print file_name
	print "$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$"
        self.config_parser.read(os.path.realpath(file_name))

    def config_sectionmap(self, section):
        contents = {}
        self.config_parser.sections()
        options = self.config_parser.options(section)

        for option in options:
            try:
                contents[option] = self.config_parser.get(section, option)

                if contents[option] == -1:
                    print('skip: %s ' % option)
            except Exception:
                print('exception on %s!' % option)
                contents[option] = None
        return contents

    def config_item(self, section, key):
        content = self.config_sectionmap(section)

        try:
	    ret_val = content[key.lower()]
            return ret_val
        except Exception:
            print('No value found for key %s under section %s' % (key, section))
            return None

if __name__ == '__main__':

	cm = ConfigManager()



