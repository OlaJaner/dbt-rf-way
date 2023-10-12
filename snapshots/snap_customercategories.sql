{% snapshot snap_customercategories %}
    {{
        config(
          target_schema='snapshots',
          unique_key='customercategory_key',
          strategy='timestamp',
          updated_at='validfrom'
        )
    }}
    -- Pro-Tip: Use sources in snapshots!
    select * from {{ ref('datastore_customercategories') }}
{% endsnapshot %}