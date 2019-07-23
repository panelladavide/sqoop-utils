import os
import shutil
from sqoop_utils import helpers, script_utils

HOME = os.path.expanduser("~")
CONFIG_DIR = os.path.join(HOME, 'sqoop_utils')
CONFIG_FILE = os.path.join(CONFIG_DIR, 'config.json')
SCRIPTS_DIR = os.path.join(CONFIG_DIR, 'scripts')
REPORTS_DIR = os.path.join(CONFIG_DIR, 'reports')

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
TEMPLATE_CONFIG = os.path.join(THIS_DIR, 'config_template.json')
EXAMPLE_SH = os.path.join(THIS_DIR, 'example_sqoop_script.sh')

def load_config():
    # Create config folder structure
    for dir in [CONFIG_DIR, SCRIPTS_DIR, REPORTS_DIR]:
        if not os.path.exists(dir):
            os.mkdir(dir)
    # Check and read config file 
    if not os.path.exists(CONFIG_FILE):
        template_on_disk = shutil.copy(TEMPLATE_CONFIG, CONFIG_DIR)
        shutil.copy(TEMPLATE_CONFIG, EXAMPLE_SH)
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
    config_data = load_config()
    for job in config_data:
        sqoop_cmd = ''
        if job['standalone']:
            sqoop_cmd = helpers.read_plain(os.path.join(SCRIPTS_DIR, job['script_name']))
            parsed = script_utils.parse_cmd(sqoop_cmd)
            print(parsed)
        else:
            sqoop_cmd = script_utils.create_cmd('','','','','','')

if __name__ == "__main__":
    main()