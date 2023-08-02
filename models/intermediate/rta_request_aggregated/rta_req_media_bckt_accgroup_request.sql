{{ config(materialized='ephemeral') }}

with req as (

   select * from {{ ref('rta_req_new_config_bckt') }}

),
stgy as (
  select * from {{ ref('glaucus_p_gl_account_decision_conf_sync_pda')}}
),
medias as(
   select * from req
   where media not in ('腾讯')
   union all
   (select dt
   ,'微信' as media
   ,REVERSE(LEFT( REVERSE(strategy_name),LOCATE('_' , REVERSE(strategy_name) )-1))config_id
   ,bucket
   ,request_pv
   ,release_pv
   ,request_uv
   ,release_uv
   from req
   where media = '腾讯'
   group by 1,2,3,4)
   union all
   (select dt
   ,'广点通' as media
   ,REVERSE(LEFT( REVERSE(strategy_name),LOCATE('_' , REVERSE(strategy_name) )-1))config_id
   ,bucket
   ,request_pv
   ,release_pv
   ,request_uv
   ,release_uv
   from req
   where media = '腾讯'
   group by 1,2,3,4)
),
request as (--RTA分桶 + 账户组粒度请求
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
   from medias left join stgy on medias.config_id = stgy.config_id and medias.bucket=stgy.bucket and stgy.input_date=date_sub(medias.dt,1))
select * from request
