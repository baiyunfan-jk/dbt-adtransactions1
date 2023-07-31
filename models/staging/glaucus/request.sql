{{ config(materialized='view') }}
with source as (

    select * from {{ source('data_ai_glaucus_preagg', 'dm_eco_bm_rta_req_new_config_bckt') }} 

),
request as (
  select
  date_format(req.dt,'yyyyMMdd')as pday,
  req.media,
  coalesce(req.bucket,'无')bucket,
  coalesce(req.config_id,'无')config_id,
  coalesce(cfg2.strategys_name,'无')strategys_name,
  request_pv,
  release_pv,
  request_uv,
  release_uv
  from(
      select dt
     ,media
     ,REVERSE(LEFT( REVERSE(strategy_name),LOCATE('_' , REVERSE(strategy_name) )-1))config_id
     ,bucket
     ,sum(request_pv)request_pv
     ,sum(release_pv)release_pv
     ,sum(request_uv)request_uv
     ,sum(release_uv)release_uv
     from fin_dm_data_ai.dm_eco_bm_rta_req_new_config_bckt
     where media not in ('腾讯' )and dt>=date_sub(date('${pDate}') ,7)--V7改: 头条和快手，加：'百度开屏' '百度百青藤' '百度'
     group by 1,2,3,4
     union all
     select dt
     ,'微信' as media
     ,REVERSE(LEFT( REVERSE(strategy_name),LOCATE('_' , REVERSE(strategy_name) )-1))config_id
     ,bucket
     ,sum(request_pv)request_pv
     ,sum(release_pv)release_pv
     ,sum(request_uv)request_uv
     ,sum(release_uv)release_uv
     from source
     where media='腾讯' and dt>=date_sub(date('${pDate}') ,7)
     group by 1,2,3,4
     union all
     select dt
     ,'广点通' as media
     ,REVERSE(LEFT( REVERSE(strategy_name),LOCATE('_' , REVERSE(strategy_name) )-1))config_id
     ,bucket
     ,sum(request_pv)request_pv
     ,sum(release_pv)release_pv
     ,sum(request_uv)request_uv
     ,sum(release_uv)release_uv
     from fin_dm_data_ai.dm_eco_bm_rta_req_new_config_bckt
     where media='腾讯' and dt>=date_sub(date('${pDate}') ,7)
     group by 1,2,3,4
  )req)
select * from request
