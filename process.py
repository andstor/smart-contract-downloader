import os
import json
import pandas as pd
import re
import requests
from os import scandir
from pathlib import Path
from tqdm import tqdm
from sys import getsizeof

def walk(path, hidden=False):
    """Recursively yield DirEntry objects for given directory."""
    for entry in scandir(path):
        if not hidden and entry.name.startswith( '.' ): continue
        try:
            is_dir = entry.is_dir(follow_symlinks=False)
        except OSError as error:
            print('Error calling is_dir():', error, file=sys.stderr)
            continue
        if is_dir:
            yield from walk(entry.path)
        else:
            yield entry

def get_chain_meta(id):
    """Get the chain id from a file name."""
    url = "https://raw.githubusercontent.com/ethereum-lists/chains/master/_data/chains/eip155-" + str(id) + ".json"
    resp = requests.get(url)
    data = json.loads(resp.text)
    return data

def path_id(text):
    return re.sub('[^0-9a-zA-Z]+', '_', text).lower()

# Precomputing files count
def count_files(path, hidden=False):
    """Count the number of files in a directory."""
    filescount = 0
    for entry in scandir(path):
        if not hidden and entry.name.startswith( '.' ): continue
        filescount += 1
    return filescount

def process_address_dir(contract_dir):
    """Process all files associated with a contract address."""
    
    contracts = []
    contract = {}
    metadata = {}
    contract['contract_address'] = contract_dir.name

    metadata_path = Path(contract_dir.path, 'metadata.json')
    try:
        with open(metadata_path) as f:
            metadata = json.load(f)
            #print("Target:", next(iter(metadata['settings']['compilationTarget'])))
            contract['contract_name'] = next(iter(metadata['settings']['compilationTarget'].items()))[1] # String
            contract['language'] = metadata['language'] # String
            contract['abi'] = json.dumps(metadata['output']['abi']) # JSON
            contract['devdoc'] = json.dumps(metadata['output']['devdoc']) # JSON
            contract['userdoc'] = json.dumps(metadata['output']['userdoc']) # JSON
            contract['compiler_version'] = metadata['compiler']['version'] # int
            contract['optimizer'] = json.dumps(metadata['settings']['optimizer']) # JSON object
            contract['evm_version'] = metadata['settings'].get('evmVersion', None) # String
        
        sources_dir = Path(contract_dir.path, 'sources')
        for file in walk(sources_dir):
            relative_path = os.path.relpath(file.path, sources_dir)
            
            source_key = [s for s in metadata['sources'].keys() if path_id(relative_path) in path_id(s)]
            if len(source_key) == 0:
                print(file.path)
                print(relative_path)
                print("-------------------------------------------------------")
                for s in metadata['sources'].keys():
                    print(s)
                    print(path_id(s))
            source_key = source_key[0]
            contract['file_path'] = source_key
            contract['license'] = metadata['sources'][source_key].get('license', None) # String
            contract['urls'] = metadata['sources'][source_key].get('urls', None) # Array of strings
            with open(file.path) as f:
                try:
                    source_code = f.read()
                except UnicodeDecodeError as error:
                    print('Error decoding file:', error)
                    print('File:', file.path)
                    continue
            contract['source_code'] = source_code
            contracts.append(contract.copy())
    except:
        print("Error processing:", contract_dir.name)
        return []

    return contracts


def process_chain_dir(chain_dir, output_dir, progress_bar):
    """Process all contracts in a chain directory."""

    chain_id = chain_dir.name
    chain_name = get_chain_meta(chain_id)['name'].replace(' ', '_').lower()
    output_dir = Path(output_dir, chain_name)
    if chain_name != 'ethereum_mainnet':
        return
    # Where to save the processed data
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    size = 0
    part = 0
    contracts = []

    files_count = count_files(chain_dir.path)
    for j, file in enumerate(scandir(chain_dir.path)):
        if not file.name.startswith( '0x' ): continue
        progress_bar.set_postfix(chain=chain_name, step = str(j) + "/" + str(files_count))
        contracts += process_address_dir(file)
        size += getsizeof(contracts)
        
        # If the size of the contracts is about > 100MB (compressed), save the contracts to a file
        max_field_size = 100000000 # 100MB
        max_field_size = max_field_size * 20 # Increase size because of "snappy" compression
        if size >= max_field_size:
            output_name = 'part.' + str(part) + '.parquet'
            contracts_df = pd.DataFrame(contracts)
            contracts_df = contracts_df.astype({
                'contract_address': "string",
                'contract_name': "string",
                'language': "string",
                'abi': "string",
                'devdoc': "string",
                'userdoc': "string",
                'compiler_version': "string",
                'optimizer': "string",
                'evm_version': "string",
                'file_path': "string",
                'license': "string",
                'source_code': "string",
                'urls': object,
            })
            contracts_df.to_parquet(Path(output_dir, output_name))
            contracts = []
            size = 0
            part += 1

    output_name = 'part.' + str(part) + '.parquet'
    contracts_df = pd.DataFrame(contracts)
    contracts_df.to_parquet(Path(output_dir, output_name))


if __name__ == '__main__':

    # Path to contracts directory containing address directories
    repo_path = Path("/Users/andrestorhaug/Downloads/k51qzi5uqu5dkuzo866rys9qexfvbfdwxjc20njcln808mzjrhnorgu5rh30lb/contracts/full_match")
    output_dir = '/Users/andrestorhaug/Downloads/contracts4/full_match'

    # Iterate over all blockchains chains
    pbar = tqdm(scandir(repo_path), total=count_files(repo_path), position=0)
    for i, chain_dir in enumerate(pbar):
        if chain_dir.name.startswith( '.' ): continue # Skip hidden files
        process_chain_dir(chain_dir, output_dir, progress_bar=pbar)
    
        #raise Exception("Done")

