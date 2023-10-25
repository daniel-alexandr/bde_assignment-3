{{
    config(
        unique_key='lga_code'
    )
}}

with

source  as (

    select * from {{ source('raw','nsw_lga_code') }}

),

unknown as (
    select
        0 as lga_code,
        'unknown' as lga_name

)
select * from unknown
union all
select * from source

