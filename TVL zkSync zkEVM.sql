WITH transfers AS (
    
    select time, 
    contract_address,
    sum(value) as value from (
    select date_trunc('day', evt_block_time) as time, contract_address, sum(cast(value as double))*-1 as value
    from erc20_ethereum.evt_Transfer
    where "from" IN(0x57891966931Eb4Bb6FB81430E6cE0A03AAbDe063,0xD7f9f54194C633F36CCD5F3da84ad4a1c38cB2cB)
    group by 1,2
    union all 
    select date_trunc('day', evt_block_time) as time, contract_address,sum(cast(value as double)) as value
    from erc20_ethereum.evt_Transfer
    where "to" in(0x57891966931Eb4Bb6FB81430E6cE0A03AAbDe063,0xD7f9f54194C633F36CCD5F3da84ad4a1c38cB2cB)
    group by 1,2
    union all 
    SELECT 
    date_trunc('day',block_time) as time, 0x0000000000000000000000000000000000000000 as contract_address,
    sum(CAST(value AS DOUBLE))*-1 AS amount
    FROM ethereum.traces tr
    WHERE "from" in( 0x32400084c286cf3e17e7b677ea9583e60a000324,0xD7f9f54194C633F36CCD5F3da84ad4a1c38cB2cB)
    AND success
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
    AND block_time > timestamp '2023-03-24'
    group by 1,2
    UNION ALL
    SELECT 
    date_trunc('day',block_time) as time, 0x0000000000000000000000000000000000000000 as contract_address, 
    sum(CAST(value AS DOUBLE)) AS amount
    FROM ethereum.traces
    WHERE to in( 0x32400084c286cf3e17e7b677ea9583e60a000324,0xD7f9f54194C633F36CCD5F3da84ad4a1c38cB2cB)
    AND success
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
    AND block_time > timestamp '2023-03-24'
    group by 1,2
    ) group by 1,2), 

days AS (
    SELECT 
        date_trunc('day',date_column) AS time, 
        contract_address
    FROM (
        VALUES
            (SEQUENCE(CAST('2023-03-24' AS timestamp), CAST(DATE_TRUNC('day', CURRENT_TIMESTAMP) AS timestamp), INTERVAL '1' DAY))
    ) AS t1(date_array)
    CROSS JOIN UNNEST(date_array) AS t2(date_column)
    CROSS JOIN (SELECT DISTINCT contract_address FROM transfers) 
),

token_all_days AS (
    SELECT
        t1.time,
        t1.contract_address,
        case when t1.contract_address=0x0000000000000000000000000000000000000000 then 0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2 else t1.contract_address end as price_contract_address,
        SUM(COALESCE(t2.value, 0)) over(partition by t1.contract_address order by t1.time) AS value
    FROM
        days as t1
        LEFT JOIN transfers  as t2 ON t1.time=t2.time and t1.contract_address=t2.contract_address
),

    prices as (
    select t1.time,t1.contract_address,
    t1.value/power(10,t2.decimals)  as value_raw,  
    (t1.value/power(10,t2.decimals))*t2.price  as value_usd, 
    case when t1.contract_address=0x0000000000000000000000000000000000000000  then 'ETH' else t2.symbol end  as symbol
    from token_all_days as t1 
    left join prices.usd as t2 on t1.time=t2.minute and t1.price_contract_address=t2.contract_address
 ), 

value_cap as (
select 
date_trunc('day',time) as time, 
contract_address,
case when value_usd<500000 then 'Other' else symbol end as symbol,
value_raw,
value_usd--, 
--sum(value_usd) over(partition by time) as total_usd
from prices
where symbol is not null 
order by 1 desc, 5 desc) 


select time, 'Total' as symbol, sum(value_usd) as value_usd
from value_cap
group by 1
order by 1 desc 
