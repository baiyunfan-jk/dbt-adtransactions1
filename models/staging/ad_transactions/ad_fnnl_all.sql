{{ config(materialized='view') }}

with source as (

   select * from {{ source('ad_transactions', 'dm_eco_bm_ad_fnnl_all') }}

),
renamed as (
select from_unixtime(unix_timestamp(pday,'yyyyMMdd'),'yyyy-MM-dd')dt
,media
,bucket
,config_id
,config_id_name
,strategys_name
,request_pv
,release_pv
,request_uv
,release_uv
,costs
,imp_pv
,imp_uv
,clk_pv
,clk_uv
,lz_uv
,wj_uv
,first_wj_uv
,wj_t0_uv
,sx_uv
,first_sx_uv
,sx_t0_uv
,sx_p18_pp_uv
,sx_p18_sc_uv
,big_sx_uv
,sum_sx_amt
,dz_uv
,dz_t0_uv
,first_sx_amt
,sx_p18_pp_amt
,sx_p18_sc_amt
,'分桶+账户组' as dimension_1--报表粒度
,'null' as dimension_2
,sum_first_dz_amt
,sx_uv_nonff
,first_sx_uv_nonff
,sum_sx_amt_nonff
,first_sx_amt_nonff
,dz_uv_nonff
,sum_first_dz_amt_nonff
,dz_t0_uv_nonff
,sum_t0_dz_amt
,sum_t0_dz_amt_nonff
 --新增
, dz_wjt0_uv
, dz_wjt0_uv_nonff
, sum_wjt0_dz_amt
, sum_wjt0_dz_amt_nonff
, dz_wjt1_uv
, dz_wjt1_uv_nonff
, sum_wjt1_dz_amt
, sum_wjt1_dz_amt_nonff
, dz_wjt3_uv
, dz_wjt3_uv_nonff
, sum_wjt3_dz_amt
, sum_wjt3_dz_amt_nonff
,imp_cpm_cost
from  source)

select * from renamed
