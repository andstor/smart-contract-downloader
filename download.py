from numpy import source
import pandas as pd
import warnings
import requests
import json
import backoff
from datasets import Dataset

warnings.simplefilter(action='ignore', category=FutureWarning)

API_KEY = "WZVVCEUJASXNTWZV81CIZGHE7KE6G5TF9K"


@backoff.on_exception(backoff.expo,
                      requests.exceptions.RequestException,
                      max_tries=8)
def get_code(address):
    url ="https://api.etherscan.io/api?module=contract&action=getsourcecode&address=" + address + "&apikey=" + API_KEY;
    with requests.get(url) as response:
        if response.status_code != 200:
            raise Exception('API response: {}'.format(response.status_code))
        data = json.loads(response.text)
        return data['result'][0]

def process_source_code(contract):
    code_format = ""
    source_code = ""
    language = ""
    # Check for Solidity Standard Json-Input format
    if contract['source_code'][:2] == "{{":
        # Fix Json by removing extranous curly brace
        source_code = contract['source_code'][1:-1]
        code_format = "JSON"
        language = "Solidity"
    elif "vyper" in contract['compiler_version']:
        source_code = contract['source_code']
        code_format = "Vyper"
        language = "Vyper"
    else:
        source_code = contract['source_code']
        code_format = "Solidity"
        language = "Solidity"
    return source_code, language, code_format


if __name__ == '__main__':
    
    # Load contract addresses
    fileinput = 'export-verified-contractaddress-opensource-license.csv'
    df = pd.read_csv(fileinput, skiprows=1, sep=',')

    # Download data
    contracts = []
    for index, row in df.iterrows():
        print("Processing: ", index + 1, " of ", len(df))

        # Get contract code from Etherscan.io
        contract = get_code(row['ContractAddress'])
        source_code, language, code_format = process_source_code(data)

        data = {
                'contract_address': row['ContractAddress'],
                'contract_name': contract['ContractName'],
                'language': language,
                'format': code_format,
                'source_code': source_code,
                'abi': contract['ABI'],
                'compiler_version': contract['CompilerVersion'],
                'optimization_used': contract['OptimizationUsed'],
                'runs': contract['Runs'],
                'constructor_arguments': contract['ConstructorArguments'],
                'evm_version': contract['EVMVersion'],
                'library': contract['Library'],
                'license_type': contract['LicenseType'],
                'proxy': contract['Proxy'],
                'implementation': contract['Implementation'],
                'tx_hash': row['Txhash'],
                'swarm_source': contract['SwarmSource']}
        contracts.append(data)
        
        # Save checkpoint
        if index % 500 == 0 and index > 0:
            pd.DataFrame(contracts).to_parquet('smart_contracts_' + str(index) + '.parquet')

    # Save data as parquet
    contracts_df = pd.DataFrame(contracts)

    # Set datatypes for columns
    contracts_df = contracts_df.astype({
        'contract_name': "string",
        'contract_address': "string",
        'source_code': "string",
        'abi': object,
        'compiler_version': "string",
        'optimization_used': bool,
        'runs': int,
        'constructor_arguments': "string",
        'evm_version': "string",
        'library': "string",
        'license_type': "string",
        'proxy': bool,
        'implementation': "string",
        'tx_hash': "string",
        'swarm_source': "string"
    })

    contracts_ds = Dataset.from_pandas(contracts_df)
    datasets = contracts_ds.train_test_split(test_size=0.05, seed=1)

    datasets['train'].to_parquet('data/all/train.parquet')
    datasets['test'].to_parquet('data/all/test.parquet')
