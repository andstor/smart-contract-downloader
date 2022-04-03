import os
import json
import pandas as pd
import re
import requests
from os import scandir
from pathlib import Path
from tqdm import tqdm
from sys import getsizeof
import math
import gc
from json.decoder import JSONDecodeError
import tarfile,os
# Precomputing files count


def count_files(path, hidden=False):
    """Count the number of files in a directory."""
    filescount = 0
    for entry in scandir(path):
        if not hidden and entry.name.startswith('.'):
            continue
        filescount += 1
    return filescount


def process_source_code(contract):
    code_format = ""
    source_code = ""
    language = ""
    # Check for Solidity Standard Json-Input format
    # print(contract)
    contract['SourceCode']
    if contract['SourceCode'][:1] == "{":
        source_code = contract['SourceCode']
        if contract['SourceCode'][:2] == "{{":
            # Fix Json by removing extranous curly brace
            source_code = source_code[1:-1]
        code_format = "JSON"
        language = "Solidity"
    elif "vyper" in contract['CompilerVersion']:
        source_code = contract['SourceCode']
        code_format = "Vyper"
        language = "Vyper"
    else:
        source_code = contract['SourceCode']
        code_format = "Solidity"
        language = "Solidity"
    return source_code, language, code_format


def process_tar_member(tar, member):
    try:
        f=tar.extractfile(member)
        data = f.read().decode("utf-8")
        data = json.loads(data)
    except:
        print("Can't decode file: " + str(member.name))
        return None
    address = Path(member.path).stem
    return process_contract_data(address, data)

def process_file(path):
    """Process a single file."""
    path = Path(path)
    try:
        with open(path, 'r') as f:
            data = json.load(f)
    except JSONDecodeError as e:
        print("Can't decode file: " + str(path))
        return None
    
    address = path.stem
    return process_contract_data(address, data)


def process_contract_data(address, data):

    if data["SourceCode"] == "":
        return None

    source_code, language, code_format = process_source_code(data)

    # TODO: extract JSON contract metadata sourcecode
    if code_format == "JSON":
        return None

    address = address

    contract = {
        'contract_name': data['ContractName'],
        'contract_address': address,
        'language': language,
        'source_code': source_code,
        'abi': data['ABI'],
        'compiler_version': data['CompilerVersion'],
        'optimization_used': data['OptimizationUsed'],
        'runs': data['Runs'],
        'constructor_arguments': data['ConstructorArguments'],
        'evm_version': data['EVMVersion'],
        'library': data['Library'],
        'license_type': data['LicenseType'],
        'proxy': data['Proxy'],
        'implementation': data['Implementation'],
        'swarm_source': data['SwarmSource']}
    return contract


def process_files(path, output_dir, parquet_size):
    """Process all files in a directory."""
    path = Path(path)
    output_dir = Path(output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    contracts = []
    contracts_count = 0
    part = 0
    empty_files = 0
    filescount = 0

    meta = {
        "contracts": 0,
        "empty": 0,
    }

    filesitter = None
    is_tar = False
    

    if path.is_dir():
        filescount = count_files(path)
        filesitter = scandir(path)
    elif tarfile.is_tarfile(path):
        is_tar = True
        tar = tarfile.open(path)
        filescount = sum(1 for member in tar if member.isfile())
        print("Tar file contains " + str(filescount) + " files")
        filesitter = tar
    else:
        print("Unknown file type: " + str(path))
        return  

    main_bar = tqdm(filesitter, total=filescount, position=0, desc="Processing")
    parquet_bar = tqdm(total=parquet_size, position=1, desc="Part " + str(part))

    for i, entry in enumerate(main_bar):
        if not entry.name.endswith('.json'):
            continue
        if is_tar:
            contract = process_tar_member(tar, entry)
        else:
            contract = process_file(entry.path)

        if contract is None:
            empty_files += 1
            meta["empty"] = str(empty_files)
            main_bar.set_postfix(meta)
            continue
        meta["empty"] = str(empty_files)

        meta["contracts"] = str(contracts_count)
        contracts.append(contract)
        main_bar.set_postfix(meta)
        parquet_bar.update(1)
        parquet_bar.set_description("Part " + str(part))

        if contracts_count % parquet_size == 0 and contracts_count != 0:
            contracts_df = pd.DataFrame(contracts)
            contracts_df = contracts_df.astype({
                'contract_name': "string",
                'contract_address': "string",
                'language': "string",
                'source_code': "string",
                'abi': "string",
                'compiler_version': "string",
                'optimization_used': bool,
                'runs': int,
                'constructor_arguments': "string",
                'evm_version': "string",
                'library': "string",
                'license_type': "string",
                'proxy': bool,
                'implementation': "string",
                'swarm_source': "string"
            })
            output_name = 'part.' + str(part) + '.parquet'
            contracts_df.to_parquet(Path(output_dir, output_name))
            contracts = []
            part += 1
            parquet_bar.reset()
            gc.collect()
        contracts_count += 1

    if len(contracts) > 0:
        output_name = 'part.' + str(part) + '.parquet'
        contracts_df = pd.DataFrame(contracts)
        contracts_df.to_parquet(Path(output_dir, output_name))
        parquet_bar.update(1)

    main_bar.close()
    parquet_bar.close()
    if is_tar:
        tar.close()
    print("-------------------------------------------------------")
    print("Processed files: " + str(filescount))
    print("Contracts: " + str(contracts_count))
    print("Empty files: " + str(empty_files))
    print("Empty percentage: " + str(round(empty_files*100/(i+1), 2)) + "%")
    print("-------------------------------------------------------")

if __name__ == '__main__':
    import argparse

    parser = argparse.ArgumentParser(
        description='Get source code of contracts')

    parser = argparse.ArgumentParser(
        description='Download contracts from Etherscan.io.')
    parser.add_argument('-s', '--source', metavar='source', type=str, required=False,
                        default="data", help='Path to directory with files to process.')
    parser.add_argument('-o', '--output', metavar='output', type=str, required=False,
                        default="output", help='The path where the output should be stored.')
    parser.add_argument('--parquet-size', metavar='parquet_size', type=int, required=False,
                        default=10000, help='The number of contracts to store in each parquet file.')
    args = parser.parse_args()

    process_files(args.source, args.output, args.parquet_size)
