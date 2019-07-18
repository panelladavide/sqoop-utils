import json
import os

CONFIG_FILE_NAME = 'config.json'
BASE_DIR = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
CONFIG_PATH = os.path.join(BASE_DIR, 'resources', CONFIG_FILE_NAME)
SCRIPT_FOLDER = os.path.join(BASE_DIR, 'resources', 'scripts')

def readJsonFile(file_path):
    with open(file_path) as json_file:
        return json.load(json_file)

def main():

    # Legge il file con le configurazione delle operazioni
    config = readJsonFile(CONFIG_PATH)

    # Separa le configurazioni che utilizzano script gia pronti da quelle che richiedono la creazioni di comandi sqoop
    ready_script_config = [x for x in config if x['script']]
    raw_script_data = [x for x in config if not x['script']]

    # Primo test di funzionamento
    print("\nREADY:\n")
    print(ready_script_config)
    print('\n')
    os.system('sh ' + os.path.join(SCRIPT_FOLDER, ready_script_config[0]['script_name']))
    print("\nTO CREATE:\n")
    print(raw_script_data)
    print('\n')

if __name__ == "__main__":
    main()