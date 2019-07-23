import os
import time
import re

THIS_DIR = os.path.dirname(os.path.realpath(__file__))

def create_cmd(table_src, table_dst, jdbc_url, username, password, parallelism):
    return "sqoop --" #TODO: implementare

def parse_output(log):
    log = log.replace('\n', ' ').replace('\t', ' ')

    result = re.search('Map input records=(.*)Map output records=', log)
    map_input_records = int(result.group(1).strip())

    result = re.search('Map output records=(.*)Input split bytes=', log)
    map_output_records = int(result.group(1).strip())

    return map_input_records, map_output_records

def get_table_name(log):
    result = re.search('--hive-table(.*)--target-dir', log)
    table_name = result.group(1).strip()
    return table_name