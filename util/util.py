import configparser
import io
import os

class Util:

    def __init__(self):
        pass

    def get_config(self):
        # Load the configuration file
        # with open("config.ini") as f:
        #     sample_config = f.read()
        d = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        filepath = os.path.join(d, "instance", "config.ini")
        config = configparser.RawConfigParser(allow_no_value=True)
        config.readfp(open(filepath))
        return config