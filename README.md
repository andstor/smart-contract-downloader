# smart-contract-scraper

> 💾 Smart contract data scraper

This is a tool for downloading verified Smart Contract data from [etherscan.io](https://etherscan.io).

To download the smart contracts stored in `export-verified-contractaddress-opensource-license.csv`, simply run `python download.py`. They will be saved to `./smart_contracts.parquet`.

A complete dataset of verified smart contracts is available at [Hugging Face](https://huggingface.co/datasets/andstor/smart_contracts).


```sql
SELECT contracts.address, COUNT(1) AS tx_count
  FROM `bigquery-public-data.crypto_ethereum.contracts` AS contracts
  JOIN `bigquery-public-data.crypto_ethereum.transactions` AS transactions 
        ON (transactions.to_address = contracts.address)
  GROUP BY contracts.address
  ORDER BY tx_count DESC
```

Total 5868827 contracts (rows)