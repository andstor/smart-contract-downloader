import os
import json
import pandas as pd
import re
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
    with open(metadata_path) as f:
        metadata = json.load(f)
        #print("Target:", next(iter(metadata['settings']['compilationTarget'])))
        contract['contract_name'] = next(iter(metadata['settings']['compilationTarget'].items()))[1] # String
        contract['language'] = metadata['language'] # String
        contract['abi'] = metadata['output']['abi'] # JSON
        contract['compiler_version'] = metadata['compiler']['version'] # int
        contract['optimizer'] = metadata['settings']['optimizer'] # JSON object
        contract['evm_version'] = metadata['settings'].get('evmVersion', None) # String
    
    sources_dir = Path(contract_dir.path, 'sources')
    try:
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
            
            contract['file_name'] = Path(source_key).name
            contract['license'] = metadata['sources'][source_key].get('license', None)
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
        return []

    return contracts


if __name__ == '__main__':

    # Path to contracts directory containing address directories
    repo_path = Path("path/to/contracts/directory")

    # Where to save the processed data
    outdir = 'path/to/save/data'
    if not os.path.exists(outdir):
        os.makedirs(outdir)

    size = 0
    part = 0
    contracts = []

    pbar = tqdm(scandir(repo_path), total=count_files(repo_path), position=0)
    for i, entry in enumerate(pbar):
        if entry.name.startswith( '.' ): continue # Skip hidden files
        

        files_count = count_files(entry.path)
        for j, file in enumerate(scandir(entry.path)):
            if not file.name.startswith( '0x' ): continue
            pbar.set_postfix(step = str(j) + "/" + str(files_count))
            contracts += process_address_dir(file)
            size += getsizeof(contracts)
            
            # If the size of the contracts is about > 100MB (compressed), save the contracts to a file
            max_field_size = 100000000 # 100MB
            max_field_size = max_field_size * 20 # Increase size because of "snappy" compression
            if size >= max_field_size:
                outname = 'part' + str(part) + '.parquet'
                contacts_df = pd.DataFrame(contracts)
                contacts_df.to_parquet(Path(outdir, outname))
                contracts = []
                size = 0
                part += 1

    outname = 'part' + str(part) + '.parquet'
    contacts_df = pd.DataFrame(contracts)
    contacts_df.to_parquet(Path(outdir, outname))
