{% snapshot snap_customers %}
    {{
        config(
          target_schema='snapshots',
          unique_key='customer_key',
          strategy='timestamp',
          updated_at='TriggerTime'
        )
    }}
    -- Pro-Tip: Use sources in snapshots!
    select * from {{ ref('datastore_customers') }}
{% endsnapshot %}