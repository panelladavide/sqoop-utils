import os
import time
import re

def create_cmd(table_src, table_dst, jdbc_url, username, password, parallelism):
    raise NotImplementedError
    return "sqoop --" #TODO: implementare

def create_count_cmd(table, url, driver, username, password):
    cmd = f""" sudo -u hdfs sqoop eval --connect "{url}" --driver  "{driver}" --username {username} --password {password} --query "SELECT count(*) FROM {table}" """
    print(cmd)
    return cmd

def parse_count_log(log):
    log = log.replace('\n', ' ').replace('\t', ' ')
    string = log.split('|')[-2]
    num = int(string)
    return num

def parse_output(log):
    log = log.replace('\n', ' ').replace('\t', ' ')

    result = re.search('Map input records=(.*)Map output records=', log)
    map_input_records = int(result.group(1).strip())

    result = re.search('Map output records=(.*)Input split bytes=', log)
    map_output_records = int(result.group(1).strip())

    return map_input_records, map_output_records

def get_table_name(cmd):
    table_name = ''

    try: 
        result = re.search('--hive-table(.*)--fields-terminated-by', cmd)
        table_name = result.group(1).strip()
    except Exception:
        try:
            result = re.search('--hive-table(.*)--target-dir', cmd)
            table_name = result.group(1).strip()
        except Exception:
            result = re.search('--table(.*)--hive-import', cmd)
            table_name = result.group(1).strip()

    return table_name

def get_table_name_oracle(cmd):
    table_name = ''

    try: 
        result = re.search('--hive-table(.*)--fields-terminated-by', cmd)
        table_name = result.group(1).strip()
    except Exception:
        try:
            result = re.search('--hive-table(.*)--target-dir', cmd)
            table_name = result.group(1).strip()
        except Exception:
            result = re.search('--table(.*)--hive-import', cmd)
            table_name = result.group(1).strip()

    prefix = ''
    

    return f"{prefix}.{table_name}"