import json
import os

CONFIG_FILE_NAME = 'config.json'
DIR_NAME = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
CONFIG_PATH = os.path.join(DIR_NAME, 'resources', CONFIG_FILE_NAME)

def readConfig():
    with open(CONFIG_PATH) as json_file:
        return json.load(json_file)

def main():
    config = readConfig()
    for table_conf in config:
        print(table_conf['script'])

if __name__ == "__main__":
    main()