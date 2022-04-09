from argparse import Namespace
from concurrent.futures import ThreadPoolExecutor, wait
from threading import Semaphore
from contracts_downloader import ContractsDownloadManager
from os import scandir
from multiprocessing import Manager

def worker(pos, sem ,args):
    with sem:
        global i
        i = 0
        downloader = ContractsDownloadManager(**vars(args), position=pos)
        downloader.download()

if __name__ == '__main__':
    import argparse
    from pathlib import Path
    import sys
    import json
    from itertools import cycle, repeat, chain
    import warnings

    parser = argparse.ArgumentParser(
        description='Orchistration tool for managing concurrent downloading of contracts from Etherscan.io.')
    parser.add_argument('-t', '--tokens', metavar='tokens', type=str,
                        default="api_keys.json", help='JSON file with Etherscan.io access tokens.')
    parser.add_argument('-a', '--addresses', metavar='addresses', type=str, required=False,
                        default="all_contracts.csv", help='CSV file containing a list of contract addresses to download.')
    parser.add_argument('-o', '--output', metavar='output', type=str, required=False,
                        default="data", help='the path where the output should be stored.')
    parser.add_argument('--shard', metavar='shard', type=int, required=True,
                        default=1, help='the number of shards to split data in.')
    parser.add_argument('--range', metavar=('start_index', 'end_index'), type=int, nargs=2, required=False,
                        default=[0,-1], help='the range of shards to proocess. Zero indexed.')
    parser.add_argument('--n-threads', metavar='n_threads', type=int, required=False,
                        default=-1, help='the n_threads to use. -1 means max.')
    parser.add_argument('--skip', metavar='skip', type=int, required=False,
                        default=0, help='the iterations to skip at start.')
    parser.add_argument('--token-multiplier', metavar='token_multiplier', type=int, required=False,
                        default=1, help='the maximum number of concurrent use of an access token.')
    args = parser.parse_args()

    with open(args.tokens) as fp:
        api_keys = json.load(fp)["keys"]
    tokens = cycle(list(chain.from_iterable(repeat(x, args.token_multiplier) for x in api_keys)))
    
    if args.n_threads == -1:
        args.n_threads = len(api_keys) * args.token_multiplier

    if args.n_threads > args.token_multiplier*len(api_keys):
        warnings.warn("The n_threads is higher than the number of tokens. Setting n_threads to " + str(args.token_multiplier*len(api_keys)))
        args.n_threads = args.token_multiplier*len(api_keys)

    start_index, end_index = args.range
    if (end_index == -1):
        end_index = args.shard - 1

    if start_index > end_index:
        raise ValueError("The start index must be less than the end index.")
    
    if (end_index - start_index) + 1 > args.shard:
        warnings.warn("The range is larger than the number of shards.")
        end_index = max(end_index + 1, args.shard) - 1

    try:
        with ThreadPoolExecutor(max_workers=args.n_threads) as executor:
            sem = Semaphore(args.n_threads)
            futures = []
            pos = 0
            for i in range(start_index, end_index+1):
                args_copy = Namespace(**vars(args))
                args_copy.index = i
                args_copy.token = next(tokens)
                future = executor.submit(worker, pos, sem, args_copy)
                #future.result()
                futures.append(future)
                pos += 1
            wait(futures)
    except Exception as e:
        print(e)

        sys.exit(1)
