from operator import index
import os
from etherscan.contracts import Contract
from etherscan.client import EmptyResponse, BadRequest, ConnectionRefused
import backoff
import json
import sys
import argparse
from pathlib import Path
import math
from tqdm import tqdm
import csv


class ContractsDownloadManager:
    def __init__(self, token, addresses="all_contracts.csv", output="data", shard=1, index=0, skip=0, position=0, **kwargs):
        self.token = token
        self.addresses_path = addresses
        self.output_dir = output
        self.shard = shard
        self.index = index
        self.skip = skip
        self.position = position

    def download(self):
        not_valid = []
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)

        not_valid = []
        if os.path.exists('not_valid.json'):
            with open('not_valid.json') as fd:
                not_valid = json.load(fd)
        count = 0
        count_effective = 0
        empty = 0

        def count_file_lines(file_path):
            with open(file_path) as fp:
                for i, l in enumerate(fp):
                    pass
            return i + 1

        adress_count = count_file_lines(self.addresses_path) - 1
        batch = math.floor(adress_count / self.shard)
        start = self.skip + (self.index * batch)
        end = batch + (self.index * batch)

        if (self.index+1) == self.shard and self.index != 0:
            prev_batch = math.floor(adress_count / (self.shard-1))
            prev_start = self.index * prev_batch
            prev_end = batch + (self.index * batch)
            batch = adress_count - prev_end
            start = prev_end
            end = batch + (self.index * prev_batch)

        if (self.index+1) > self.shard:
            raise ValueError("Index out of range")

        if (start > adress_count):
            raise ValueError("Start out of range")
        
        with open(self.addresses_path) as fp:
            reader = csv.reader(fp)
            pbar = tqdm(total=batch, position=self.position,
                        desc="Shard " + str(self.index+1) + "/" + str(self.shard), initial=self.skip)
            for i, line in enumerate(reader):
                if (count >= end):
                    break
                count += 1
                if start > count:
                    continue
                address_path = line[0]
                if address_path in not_valid:
                    continue
                pbar.update(1)  # update progress bar    
                contract_path = Path(self.output_dir, address_path + '.json')
                meta = {}
                meta["token"] = self.token
                meta["index"] = str(count) + "/" + str(adress_count)
                if os.path.exists(contract_path):
                    pbar.set_postfix(meta)
                    continue
                count_effective += 1
                meta["empty"] = str(round(empty*100/count_effective, 2)) + "%"
                pbar.set_postfix(meta)
                try:
                    sourcecode = self.download_contract(address=address_path)
                    if len(sourcecode[0]['SourceCode']) == 0:
                        empty += 1
                    with open(contract_path, 'w') as fd:
                        json.dump(sourcecode[0], fd)

                except Exception as identifier:
                    not_valid.append(address_path)
                    with open('not_valid.json', 'w') as fd:
                        json.dump(not_valid, fd)
                    print(identifier)
    

    @backoff.on_exception(backoff.expo,
                          (EmptyResponse, BadRequest, ConnectionRefused),
                          max_tries=8)
    def download_contract(self, address):
        api = Contract(address=address, api_key=self.token)
        sourcecode = api.get_sourcecode()
        return sourcecode


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Get source code of contracts')

    parser = argparse.ArgumentParser(
        description='Download contracts from Etherscan.io.')
    parser.add_argument('-t', '--token', metavar='token',
                        type=str, help='Etherscan.io API key.')
    parser.add_argument('-a', '--addresses', metavar='addresses', type=Path, required=False,
                        default="contract_addresses.csv", help='CSV file containing a list of contract addresses to download.')
    parser.add_argument('-o', '--output', metavar='output', type=Path, required=False,
                        default="output", help='the path where the output should be stored.')
    parser.add_argument('--shard', metavar='shard', type=int, required=False,
                        default=1, help='the number of shards to split data in.')
    parser.add_argument('--index', metavar='index', type=int, required='--shard' in sys.argv,
                        default=0, help='the index of the shard to process. Zero indexed.')
    parser.add_argument('--skip', metavar='skip', type=int, required=False,
                        default=0, help='the lines to skip reading from in the address list.')
    args = parser.parse_args()

    token = args.token
    addresses_path = args.addresses.resolve()
    output_dir = args.output.resolve()
    shard = args.shard
    index = args.index
    skip = args.skip

    cdm = ContractsDownloadManager(
        token, addresses_path, output_dir, shard, index, skip)
    cdm.download()
