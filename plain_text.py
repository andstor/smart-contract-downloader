from datasets import Dataset, concatenate_datasets
from split_files import split_files

if __name__ == '__main__':

    train_ds = Dataset.from_parquet('data/all/train.parquet')
    test_ds = Dataset.from_parquet('data/all/dev.parquet')
    contracts_df = concatenate_datasets([train_ds, test_ds]).to_pandas()
    files_df = split_files(contracts_df)

    # Make multiindex
    files_df = files_df.set_index(['contract_address', 'file_name'])
    
    # Drop duplicates of same contract (libraries)
    dupes = files_df.index.get_level_values('file_name').duplicated(keep='first')
    dupes[files_df.index.get_level_values('file_name') == ''] = False
    files_df = files_df[~dupes] # Keep single file contracts
    files_df.index.get_level_values('file_name').value_counts()

    # Rename "source_code" column to "text"
    files_df = files_df.rename(columns={'source_code': 'text'})

    # Subset of df containing text (source code) and language
    files_df = files_df.reset_index()[['text', 'language']].reset_index(drop=True)

    contracts_ds = Dataset.from_pandas(files_df)
    datasets = contracts_ds.train_test_split(test_size=0.05, seed=1)

    datasets['train'].to_parquet('data/plain_text/train.parquet')
    datasets['test'].to_parquet('data/plain_text/dev.parquet')