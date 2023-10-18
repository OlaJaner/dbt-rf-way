{% snapshot snap_people %}
    {{
        config(
          target_schema='snapshots',
          unique_key='personid',
          strategy='timestamp',
          updated_at='UPDATEDAT'
        )
    }}
    -- Pro-Tip: Use sources in snapshots!
    select * from {{ ref('stg_people') }}
{% endsnapshot %}