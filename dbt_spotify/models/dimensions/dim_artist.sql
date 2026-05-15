-- dim_artist: one row per unique artist
-- SCD Type 1 — latest values win on each run
{{
    config(
        materialized='table',
        unique_key='artist_id'
    )
}}

with source as (
    select distinct
        artist_name
    from {{ ref('stg_tracks_typed') }}
    where artist_name is not null
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['artist_name']) }} as artist_id,
        artist_name,
        now()                                                    as updated_at
    from source
)

select * from final
