import pandas as pd
import warnings
import requests
import json
import backoff

warnings.simplefilter(action='ignore', category=FutureWarning)

API_KEY = "ETHERSCAN_API_KEY"


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
        
        
        data = {'Txhash': row['Txhash'],
                'ContractAddress': row['ContractAddress'],
                'ContractName': contract['ContractName'],
                'SourceCode': contract['SourceCode'],
                'ABI': contract['ABI'],
                'CompilerVersion': contract['CompilerVersion'],
                'OptimizationUsed': contract['OptimizationUsed'],
                'Runs': contract['Runs'],
                'ConstructorArguments': contract['ConstructorArguments'],
                'EVMVersion': contract['EVMVersion'],
                'Library': contract['Library'],
                'LicenseType': contract['LicenseType'],
                'Proxy': contract['Proxy'],
                'Implementation': contract['Implementation'],
                'SwarmSource': contract['SwarmSource']}
        contracts.append(data)
        
        # Save checkpoint
        if index % 500 == 0 and index > 0:
            pd.DataFrame(contracts).to_parquet('smart_contracts_' + str(index) + '.parquet')

    # Save data as pickle
    contracts_df = pd.DataFrame(contracts)
    contracts_df.to_parquet('smart_contracts.parquet')
