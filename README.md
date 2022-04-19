# smart-contract-downloader

> 💾 Download smart contracts from Etherscan.io

This is a tool for downloading verified Smart Contract data from [etherscan.io](https://etherscan.io).

A complete dataset of verified smart contracts is available at 🤗 [Hugging Face](https://huggingface.co/datasets/andstor/smart_contracts), downloaded :black_joker: 1st of April 2022.

## Requirements
### Dependencies
Install the Python dependencies defined in the requirements.txt.
```shell
pip install -r requirements.txt
```

### Etherscan API access
In order to gain access to the Etherscan.io API, you will need an Etherscan [acount](https://docs.etherscan.io/getting-started/creating-an-account) and generate [API key(s)](https://docs.etherscan.io/getting-started/viewing-api-usage-statistics). If using [orchestration](#orchestration), the API key(s) needs to be added to the `api_keys.json` file.

### Collect contract addresses
To download the smart contracts from Etherscan, a collection of the wanted contract addresses is needed (CSV file). See example [contract_addresses.csv](https://huggingface.co/datasets/andstor/smart_contracts/blob/main/contract_addresses.csv).

To collect such addresses, one can for example make use of Google BigQuery. The following query will select all the contracts that have at least one transaction.

```sql
SELECT contracts.address, COUNT(1) AS tx_count
  FROM `bigquery-public-data.crypto_ethereum.contracts` AS contracts
  JOIN `bigquery-public-data.crypto_ethereum.transactions` AS transactions 
        ON (transactions.to_address = contracts.address)
  GROUP BY contracts.address
  ORDER BY tx_count DESC
```

## Contracts downloader
The `contracts_downloader.py` script will try to download all the verified smart contract data from the available on [etherscan.io](https://etherscan.io), whos address is in the address list (CSV file). Each contract address will producee a JSON file with the name of the address, conttaining the data provided by Etherscan. Addresses that has not yet been verified on Etherscan will still produce an JSON file with mostly empty data fields.

### Usage

```script
usage: contracts_downloader.py [-h] [-t token] [-a addresses] [-o output] [--shard shard] [--index index] [--skip skip]

Download contracts from Etherscan.io.

optional arguments:
  -h, --help            show this help message and exit
  -t token, --token token
                        Etherscan.io API key.
  -a addresses, --addresses addresses
                        CSV file containing a list of contract addresses to download.
  -o output, --output output
                        the path where the output should be stored.
  --shard shard         the number of shards to split data in.
  --index index         the index of the shard to process. Zero indexed.
  --skip skip           the lines to skip reading from in the address list.
```

### Example
To download the smart contracts whose address is in `contract_addresses.csv`, run:
```
python script/contracts_downloader.py -token <API_KEY> --addresses contract_addresses.csv
```
The contracts will be saved to `./output` as default.

## Orchestration
In order to speed up the downloading process, an orchestration script is provided. This enables multithreaded downloading from Etherscan, using multiple API keys. In order for this to be effective, the contract address list needs to be split into multiple logical shards.

### Usage
```script
usage: orchestrate.py [-h] [-t tokens] [-a addresses] [-o output] --shard shard [--range start_index end_index] [--n-threads n_threads] [--skip skip] [--token-multiplier token_multiplier]

Orchistration tool for managing concurrent downloading of contracts from Etherscan.io.

optional arguments:
  -h, --help            show this help message and exit
  -t tokens, --tokens tokens
                        JSON file with Etherscan.io access tokens.
  -a addresses, --addresses addresses
                        CSV file containing a list of contract addresses to download.
  -o output, --output output
                        the path where the output should be stored.
  --shard shard         the number of shards to split data in.
  --range start_index end_index
                        the range of shards to proocess. Zero indexed.
  --n-threads n_threads
                        the n_threads to use. -1 means max.
  --skip skip           the iterations to skip at start.
  --token-multiplier token_multiplier
                        the maximum number of concurrent use of an access token.
```
### Example

To start a distributed downloading with 5 threads, each with it's own API key defined in `api_keys.json`, run:
```
python script/contracts_downloader.py --shard 5 --addresses contract_addresses.csv
```

To limit the number of threads, just pass the `--n-threads` argument.
```
python script/contracts_downloader.py --n-threads 2 --shard 5 --addresses contract_addresses.csv
```
This will use a maximum of two concurrent threads for downloading. If number of API keys is > `n-threads` each new shard/thread will pick the next key from the list.

## License

Copyright © [André Storhaug](https://github.com/andstor)

smart-contract-downloader is licensed under the [MIT License](https://github.com/andstor/smart-contract-downloader/blob/main/LICENSE).  
