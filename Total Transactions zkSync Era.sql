SELECT
  count(distinct hash) as txs,
  count(distinct "from") as users
FROM
  zksync.transactions
