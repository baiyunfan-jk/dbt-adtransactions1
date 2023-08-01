{{ config(materialized='ephemeral') }}

with source as (

   select * from {{ ref('rta_bucket_cfgid_cost') }}

),
stgy as (
  select * from {{ ref('glaucus_p_gl_account_decision_conf_sync_pda')}}
),
spend as (
select * from source
left join stgy cfg2
    on a.config_id = cfg2.config_id and a.bucket=cfg2.bucket --and date_format(a.dt,'yyyyMMdd')=cfg2.input_pday
    and from_unixtime(unix_timestamp(cfg2.input_pday,'yyyyMMdd'),'yyyy-MM-dd')=date_sub(a.dt,1)
where dt >=date_sub(date('${pDate}') ,7)
)
