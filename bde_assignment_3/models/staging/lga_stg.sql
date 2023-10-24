{{
    config(
        unique_key='lga_code'
    )
}}


select * from {{ source('raw', 'listings') }}
