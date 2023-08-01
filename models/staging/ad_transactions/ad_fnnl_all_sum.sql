{{ config(materialized='view') }}

with source as (

   select * from {{ ref('ad_fnnl_all') }}

),
renamed as(
select from_unixtime(unix_timestamp(pday,'yyyyMMdd'),'yyyy-MM-dd')dt
,media
,'' as bucket
,config_id
,config_id_name
,strategys_name
,sum(request_pv) request_pv
,sum(release_pv)release_pv
,sum(request_uv)request_uv
,sum(release_uv)release_uv
,sum(costs)costs
,sum(imp_pv)imp_pv
,sum(imp_uv)imp_uv
,sum(clk_pv)clk_pv
,sum(clk_uv)clk_uv
,sum(lz_uv)lz_uv
,sum(wj_uv)wj_uv
,sum(first_wj_uv)first_wj_uv
,sum(wj_t0_uv)wj_t0_uv
,sum(sx_uv)sx_uv
,sum(first_sx_uv)first_sx_uv
,sum(sx_t0_uv)sx_t0_uv
,sum(sx_p18_pp_uv)sx_p18_pp_uv
,sum(sx_p18_sc_uv)sx_p18_sc_uv
,sum(big_sx_uv)big_sx_uv
,sum(sum_sx_amt)sum_sx_amt
,sum(dz_uv)dz_uv
,sum(dz_t0_uv)dz_t0_uv
,sum(first_sx_amt)first_sx_amt
,sum(sx_p18_pp_amt)sx_p18_pp_amt
,sum(sx_p18_sc_amt)sx_p18_sc_amt
,'账户组' as dimension_1--报表粒度
,'null' as dimension_2
,sum(sum_first_dz_amt)sum_first_dz_amt
,sum(sx_uv_nonff)sx_uv_nonff
,sum(first_sx_uv_nonff)first_sx_uv_nonff
,sum(sum_sx_amt_nonff)sum_sx_amt_nonff
,sum(first_sx_amt_nonff)first_sx_amt_nonff
,sum(dz_uv_nonff)dz_uv_nonff
,sum(sum_first_dz_amt_nonff)sum_first_dz_amt_nonff
,sum(dz_t0_uv_nonff)dz_t0_uv_nonff
,sum(sum_t0_dz_amt)sum_t0_dz_amt
,sum(sum_t0_dz_amt_nonff)sum_t0_dz_amt_nonff
--新增
,sum(dz_wjt0_uv) dz_wjt0_uv
,sum(dz_wjt0_uv_nonff) dz_wjt0_uv_nonff
,sum(sum_wjt0_dz_amt) sum_wjt0_dz_amt
,sum(sum_wjt0_dz_amt_nonff) sum_wjt0_dz_amt_nonff
,sum(dz_wjt1_uv) dz_wjt1_uv
,sum(dz_wjt1_uv_nonff) dz_wjt1_uv_nonff
,sum(sum_wjt1_dz_amt) sum_wjt1_dz_amt
,sum(sum_wjt1_dz_amt_nonff) sum_wjt1_dz_amt_nonff
,sum(dz_wjt3_uv) dz_wjt3_uv
,sum(dz_wjt3_uv_nonff) dz_wjt3_uv_nonff
,sum(sum_wjt3_dz_amt) sum_wjt3_dz_amt
,sum(sum_wjt3_dz_amt_nonff) sum_wjt3_dz_amt_nonff
,sum(imp_cpm_cost) imp_cpm_cost
from  source
