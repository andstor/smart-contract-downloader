import json
import re
import pandas as pd

def split_solidity_files(record):
    files = []
    files_code = re.split('\/\/ File:? (.*?\.sol)', record['source_code'])
    for index, file in enumerate(files_code):
        if index % 2 == 0:
            continue

        filename = ''
        if len(file) > 1:
            # File name is every other element in files_code array, except for the first element.
            filename = file
            source_code = files_code[index+1].strip()
        else:
            source_code = file

        files.append({'file_name': filename, 'source_code': source_code})

    return files


def split_json_files(record):
    files = []
    record_json = json.loads(record['source_code'])
    files_code = record_json['sources']
    for filename, source_code in files_code.items():
        files.append({'file_name': filename, 'source_code': source_code})
    return files


def split_files(df):
    contracts = []

    for _, row in df.iterrows():
        record = row.to_dict()
        if record['format'] == 'Solidity':
            files = split_solidity_files(record)
            for item in files:
                if item['source_code'] == '':
                    continue
                record['file_name'] = item['file_name']
                record['source_code'] = item['source_code']
                contracts.append(record.copy())
        elif record['format'] == 'JSON':
            files = split_json_files(record)
            for item in files:
                if item['source_code'] == '':
                    continue
                record['file_name'] = item['file_name']
                record['source_code'] = item['source_code']['content']
                contracts.append(record.copy())
        else:
            record['file_name'] = ""
            contracts.append(record.copy())

    contracts_df = pd.DataFrame(contracts)
    return contracts_df
