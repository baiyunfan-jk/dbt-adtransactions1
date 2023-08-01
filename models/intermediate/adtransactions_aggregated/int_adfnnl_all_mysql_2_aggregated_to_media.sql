
{{ config(
  materialized = 'incremental',
  incremental_strategy = 'insert_overwrite',
)}}
with source as {{ref('ad_fnnl_all')}},
incremental_table_data as (
  select a.*,b.min_dt,b.max_dt
  from source a
  left join (
  select media,config_id_name,strategys_name,
  min(dt) min_dt,
  max(dt) max_dt,
  from source
  group by 1,2,3
  )b on a.media=b.media and a.config_id_name=b.config_id_name and a.strategys_name=b.strategys_name
)
select * from incremental_table_data
