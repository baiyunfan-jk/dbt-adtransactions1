{{ config(materialized='ephemeral') }}

with req as (

   select * from {{ ref('rta_req_new_config_bckt') }}

),
stgy as (
  select * from {{ ref('glaucus_p_gl_account_decision_conf_sync_pda')}}
),
