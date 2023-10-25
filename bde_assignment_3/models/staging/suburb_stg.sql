{{
    config(
        unique_key='suburb_name'
    )
}}

with

source  as (

    select * from {{ source('raw','nsw_lga_suburb') }}

),

unknown as (
    select
        'unknown' as lga_name,
        'unknown' as suburb_name
)

select * from source
union all
select * from unknown


