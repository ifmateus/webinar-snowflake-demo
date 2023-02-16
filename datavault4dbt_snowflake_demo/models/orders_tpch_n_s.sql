{{ config(schema='turbovault_rdv',
           materialized='view') }} 

{%- set yaml_metadata -%}
sat_v0: 'orders_tpch_n0_s'
hashkey: "hk_orders_h"
hashdiff: 'hd_orders_tpch_n_s'
{%- endset -%}

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ datavault4dbt.sat_v1(sat_v0=metadata_dict['sat_v0'],
          hashkey=metadata_dict['hashkey'],
          hashdiff=metadata_dict['hashdiff'],
          ledts_alias=none) }}