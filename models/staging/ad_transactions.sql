{{ config(materialized='view') }}
  
with renamed as (
    select p_day, p_resource_code, win_config_id, req_bucket,
        sum(imp_pv) as imp_pv,
        count(distinct imp_device_md5) as imp_uv,
        sum(clk_pv) as clk_pv,
        count(distinct clk_device_md5) as clk_uv,
        sum(imp_cpm_cost) as imp_cpm_cost
    from (
    select *
         , case when length(imp_device_id)=32 then imp_device_id
           when length(imp_oaid_md5)=32 then imp_oaid_md5
           when length(imp_android_id)=32 then imp_android_id
          else null end as imp_device_md5--集中设备号到一个字段,计算UV
         , case when length(clk_device_id)=32 then clk_device_id
           when length(clk_oaid_md5)=32 then clk_oaid_md5
           when length(clk_android_id)=32 then clk_android_id
          else null end as clk_device_md5--集中设备号到一个字段,计算UV
        , sum(imp_agg_ct) as imp_pv
        , sum(clk_agg_ct) as clk_pv
        , sum(imp_cpm_cost) as imp_cpm_cost
    from fin_dm_data_ai.dm_eco_dataeco_ad_transations_0_4_1
    where p_day >=date_format(date_sub(date('${pDate}') ,7),'yyyyMMdd')
    )

  )
select * from renamed
