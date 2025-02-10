import configparser
import os

def get_config():
    config_path = os.path.join(os.path.dirname(__file__), '../connection.conf')
    parser = configparser.ConfigParser()
    parser.read(config_path)
    return parser
