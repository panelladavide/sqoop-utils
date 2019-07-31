import os
import shutil
import csv
import subprocess
import time
from sqoop_utils import helpers, script_utils

HOME = os.path.expanduser("~")
CONFIG_DIR = os.path.join(HOME, 'sqoop_utils')
DBS_CONFIG_FILE = os.path.join(CONFIG_DIR, 'dbs.json')
JOBS_CONFIG_FILE = os.path.join(CONFIG_DIR, 'jobs.json')
SCRIPTS_DIR = os.path.join(CONFIG_DIR, 'scripts')
REPORTS_DIR = os.path.join(CONFIG_DIR, 'reports')
MULTISCRIPT_SAP = os.path.join(CONFIG_DIR, 'multiscript_sap.sh')
MULTISCRIPT_ORACLE = os.path.join(CONFIG_DIR, 'multiscript_oracle.sh')

THIS_DIR = os.path.dirname(os.path.realpath(__file__))
DBS_TEMPLATE_CONFIG = os.path.join(THIS_DIR, 'dbs.template.json')
JOBS_TEMPLATE_CONFIG = os.path.join(THIS_DIR, 'jobs.template.json')

# Check and read config file 
def load_config(what, where, template):
    if not os.path.exists(where):
        # TODO: don't overwrite previous template file in the folder
        template_on_disk = shutil.copy(template, CONFIG_DIR)
        message = f"""
        {what} configuration file not found!!
        Placed a template in {template_on_disk}
        Fill it with your data and rename it to "config.json"
        """
        print(message)
        return None
    else:
        return helpers.read_json(where)

# Spin
def main():
    skip_script = True # TODO: parse from command line
    skip_custom = True # TODO: parse from command line
    skip_standalone_cmd_sap = False # TODO: parse from command line
    skip_standalone_cmd_oracle = False # TODO: parse from command line

    # Check weird case
    if skip_script and skip_custom and skip_standalone_cmd_sap and skip_standalone_cmd_oracle:
        print("Why do you skip all jobs?")
        exit(0)

    # Create config folder structure if not present
    for dir in [CONFIG_DIR, SCRIPTS_DIR, REPORTS_DIR]:
        if not os.path.exists(dir):
            os.mkdir(dir)

    # Load configuration from json file or initialize templates
    db_data = load_config('Databases', DBS_CONFIG_FILE, DBS_TEMPLATE_CONFIG)
    config_data = load_config('Jobs', JOBS_CONFIG_FILE, JOBS_TEMPLATE_CONFIG)

    # Exit if configuration is not complete
    if db_data is None or config_data is None:
        exit(0)

    # TODO: check configuration consistency

    # Load Sap Data
    multiscript_sap = helpers.read_plain(MULTISCRIPT_SAP)
    lines_sap = [x for x in multiscript_sap.split('\n') if not x=='']
    sap_data = [{'type': 'standalone_cmd_sap', 'cmd': x} for x in lines_sap]

    # Load Oracle Data
    multiscript_oracle = helpers.read_plain(MULTISCRIPT_ORACLE)
    lines_oracle = [x for x in multiscript_oracle.split('\n') if not x=='']
    oracle_data = [{'type': 'standalone_cmd_oracle', 'cmd': x} for x in lines_oracle]

    # Total Data
    total_data = sap_data + oracle_data # + config_data

    # Initialize CSV report file
    current_millis = int(round(time.time() * 1000))
    output_file_name = '.'.join([str(current_millis), 'csv'])
    OUTPUT_FILE = os.path.join(REPORTS_DIR, output_file_name)
    #TODO: check file not exists

    #TODO: report to classe e current report

    csv_fieldnames = [
        'table_name',
        'before_count',
        'before_count_ts',
        'after_count',
        'after_count_ts',
        'start_timestamp',
        'end_timestamp',
        'map_input_records',
        'map_output_records'    
        ]
    with open(OUTPUT_FILE, mode='a+') as csv_file:
        writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
        writer.writeheader()

    # Process configured sqoop jobs
    for job in total_data:
        sqoop_cmd = ''

######### TODO: 1 fase: leggere dai ref ################################
######### TODO: 2 fase: possibilente leggere dal parse del comando #####
        table_name = ''
        url = ''
        driver = ''
        username = ''
        password = ''
########################################################################
######### TODO: e alla fine dello if-switch, anzi fare process (cmd, table, ecc) #########

        if job['type'] == "standalone_cmd_sap":
            sqoop_cmd = job['cmd']
            table_name = script_utils.get_table_name(sqoop_cmd)

        elif job['type'] == "standalone_cmd_oracle":
            sqoop_cmd = job['cmd']
            table_name = script_utils.get_table_name_oracle(sqoop_cmd)

        elif job['type'] == "script":
            if skip_script:
                continue
            raise NotImplementedError
            script_file_path = os.path.join(SCRIPTS_DIR, job['script_name'])
            sqoop_cmd = helpers.read_plain(script_file_path)
            table_name = script_utils.get_table_name(sqoop_cmd)
        elif job['type'] == "multiscript":
            raise NotImplementedError
        elif job['type'] == "cmd":
            raise NotImplementedError 
        elif job['type'] == "custom":
            if skip_custom:
                continue
            raise NotImplementedError
            table_name = job['data']['table_source']
            sqoop_cmd = script_utils.create_cmd('','','','','','') # TODO: riempire 
        else:
            print('\nNot recognized type of:\n')
            print(job)
            print('\n')

        record = {}
        try:
            
            # Create the count command
            count_cmd = script_utils.create_count_cmd(table_name, url, driver, username, password)

            ### Count Before
            child_before_count_process = subprocess.run(['sh','-c', count_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
            before_count = script_utils.parse_count_log(child_before_count_process.stdout)
            print(child_before_count_process.stdout)
            print(child_before_count_process.stderr)
            if (child_before_count_process.returncode is not 0):
                raise Exception
            before_count_ts = int(round(time.time() * 1000))

            ### Run process
            start_timestamp = int(round(time.time() * 1000))
            child_process = subprocess.run(['sh','-c', sqoop_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
            (m_in, m_out) = (-999, -999) #script_utils.parse_output(child_process.stdout)
            print(child_process.stdout)
            print(child_process.stderr)
            if (child_process.returncode is not 0):
                raise Exception
            end_timestamp = int(round(time.time() * 1000))

            ### Count After
            child_after_count_process = subprocess.run(['sh','-c', count_cmd], stdout=subprocess.PIPE, stderr=subprocess.PIPE, encoding="utf-8")
            after_count = script_utils.parse_count_log(child_after_count_process.stdout)
            print(child_after_count_process.stdout)
            print(child_after_count_process.stderr)
            if (child_after_count_process.returncode is not 0):
                raise Exception
            after_count_ts = int(round(time.time() * 1000))

            # Create the CSV entry
            record = {
                "table_name": table_name,
                'before_count': before_count,
                'before_count_ts': before_count_ts,
                'after_count': after_count,
                'after_count_ts': after_count_ts,
                'start_timestamp': start_timestamp,
                'end_timestamp': end_timestamp,
                "map_input_records": m_in,
                "map_output_records": m_out
            }

        except Exception as err:
            record = {
                "table_name": table_name,
                'before_count': -1,
                'before_count_ts': -1,
                'after_count': -1,
                'after_count_ts': -1,
                'start_timestamp': -1,
                'end_timestamp': -1,
                "map_input_records": -1,
                "map_output_records": -1
            }
            print(err)
            print('\nError during job:\n')
            print(job)
            print('\n')

        # Write to the CSV report file
        with open(OUTPUT_FILE, mode='a+') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=csv_fieldnames)
            writer.writerow(record)


if __name__ == "__main__":
    main()