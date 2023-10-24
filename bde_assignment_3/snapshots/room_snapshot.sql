{% snapshot snapshot_room %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='host_id',
          updated_at='scraped_date'
        )
    }}

select scraped_date,
host_id,
room_type

 from {{ source('raw', 'listings') }}


{% endsnapshot %}