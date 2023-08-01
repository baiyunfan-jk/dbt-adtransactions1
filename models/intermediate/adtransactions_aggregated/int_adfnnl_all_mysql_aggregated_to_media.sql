{{ config(
  materialized = 'incremental',
  incremental_strategy = 'insert_overwrite',
)}}
with source as {{ref('ad_fnnl_all')}},
incremental_table_data as (
  select *
  from source a
  inner join
  (select media,config_id,sum(imp_pv)imps from source
  where media in ('广点通','微信')
  group by 1,2)tx
  on a.media=tx.media and a.config_id=tx.config_id
  where a.media in ('广点通','微信') and tx.imps>0
  union all
  select *
  from source
  where (media ='穿山甲' and config_id in ('42','13')
  or (media ='头条' and config_id not in ('42','13'))
  or (media ='百度开屏' and config_id in ('59','60'))--V7
  or (media in('快手' ,'百度') and config_id not in ('59','60'))--V7
)
  group by dt
  ,media
  ,config_id
  ,config_id_name
  ,strategys_name
  ,dimension_1
  ,dimension_2)
select * from incremental_table_data
