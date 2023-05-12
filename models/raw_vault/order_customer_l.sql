{{ config(materialized='incremental',
          schema='Core') }}

{%- set yaml_metadata -%}
source_models: 
    stg_order:
        link_hk: hk_order_customer_l
        rsrc_static: 'TPC_H_SF1.Orders'
    stg_nation:
        link_hk: hk_nation_region_l
        fk_columns: 
            - hk_nation_h
            - hk_region_h
        rsrc_static: 'TPC_H_SF1.Nation'
link_hashkey: hk_order_customer_l
foreign_hashkeys: 
    - hk_order_h
    - hk_customer_h
{%- endset -%}      

{% set metadata_dict = fromyaml(yaml_metadata) %}

{{ datavault4dbt.link(source_models=metadata_dict['source_models'],
                     link_hashkey=metadata_dict['link_hashkey'],
                     foreign_hashkeys=metadata_dict['foreign_hashkeys']) }}