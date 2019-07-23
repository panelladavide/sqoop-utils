import os
import shutil
import subprocess
from sqoop_utils import helpers, script_utils

HOME = os.path.expanduser("~")
CONFIG_DIR = os.path.join(HOME, 'sqoop_utils')
CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.json')
SCRIPTS_DIR = os.path.join(CONFIG_DIR, 'scripts')
REPORTS_DIR = os.path.join(CONFIG_DIR, 'reports')

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_CONFIG = os.path.join(THIS_DIR, 'config_template.json')

def load_config():
    # Create config folder structure
    for dir in [CONFIG_DIR, SCRIPTS_DIR, REPORTS_DIR]:
        if not os.path.exists(dir):
            os.mkdir(dir)
    # Check and read config file 
    if not os.path.exists(CONFIG_FILE):
        template_on_disk = shutil.copy(TEMPLATE_CONFIG, CONFIG_DIR)
        message = f"""
        Configuration file not found!!
        Placed a template in {template_on_disk}
        Fill it with your data and rename it to "config.json"
        """
        print(message)
        exit(1)
    else:
        return helpers.read_json(CONFIG_FILE)

def main():
    skip_standalone = True # TODO: parse from command line
    skip_custom = False # TODO: parse from command line

    # Load configuration 
    config_data = load_config()

    # Check weird case
    if skip_standalone and skip_custom:
        print("Why do you skip all jobs?")
        exit(0)
    
    # Process configured sqoop jobs
    for job in config_data:
        sqoop_cmd = ''
        table_name = ''
        if job['standalone']:
            if skip_standalone:
                continue
            script_file_path = os.path.join(SCRIPTS_DIR, job['script_name'])
            sqoop_cmd = helpers.read_plain(script_file_path)
            table_name = script_utils.get_table_name(sqoop_cmd)
        else:
            if skip_custom:
                continue
            table_name = job['data']['table_source']
            sqoop_cmd = script_utils.create_cmd('','','','','','') # TODO: riempire
            raise NotImplementedError

        child_process = subprocess.run(['sh','-c', sqoop_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
        (m_in, m_out) = script_utils.parse_output(child_process.stdout)
        print(table_name, m_in, m_out)

    # Write the output report
    # TODO:

if __name__ == "__main__":
    main()