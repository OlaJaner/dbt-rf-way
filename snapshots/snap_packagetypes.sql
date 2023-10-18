{% snapshot snap_packagetypes %}
    {{
        config(
          target_schema='snapshots',
          unique_key='PACKAGETYPEID',
          strategy='timestamp',
          updated_at='UpdateAt'
        )
    }}
    -- Pro-Tip: Use sources in snapshots!
    select * from {{ ref('stg_packagetypes') }}
{% endsnapshot %}