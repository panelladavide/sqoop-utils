import json

def read_plain(file_path):
    with open(file_path, 'r', encoding='utf-8') as template_file:
        return template_file.read()

def read_json(file_path):
    with open(file_path, 'r') as json_file:
        return json.load(json_file)