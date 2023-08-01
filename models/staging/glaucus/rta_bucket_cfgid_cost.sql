{{ config(materialized='view') }}

with source as (

    select * from {{ source('data_ai_glaucus_preagg', 'dm_eco_bm_rta_bucket_cfgid_cost') }} 

),
spend as (
select date_format(dt,'yyyyMMdd')as pday,
case when qd='微信MP' then '微信' else qd end as media,--广点通,微信MP,穿山甲,头条,null,快手
  coalesce(a.bucket,'无')bucket,
  coalesce(a.config_id,'无')config_id,
  coalesce(cfg2.strategys_name,'无')strategys_name,
costs
from source)
select * from spend
