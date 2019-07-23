#!/usr/bin/env python
import json
import os
from sqoop_scripts.sqoop_scripts_launcher import execute_sqoop_scripts

CONFIG_FILE_NAME = 'config.json'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'resources', CONFIG_FILE_NAME)
SCRIPT_FOLDER = os.path.join(BASE_DIR, 'resources', 'scripts')

def readJsonFile(file_path):
    with open(file_path, 'r') as json_file:
        return json.load(json_file)

def main():

    # Legge il file con le configurazione delle operazioni
    config = readJsonFile(CONFIG_PATH)

    # Separa le configurazioni che utilizzano script gia pronti da quelle che richiedono la creazioni di comandi sqoop
    ready_script_config = [x for x in config if x['script']]
    raw_script_data = [x for x in config if not x['script']]

    # Primo test di funzionamento con il file di test
    script_file_list = [os.path.join(SCRIPT_FOLDER, x['script_name']) for x in ready_script_config]
    execute_sqoop_scripts(script_file_list, BASE_DIR)

if __name__ == "__main__":
    main()