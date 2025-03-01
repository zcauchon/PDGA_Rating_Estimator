{{ config(materialized = 'ephemeral') }}

select distinct
    player_pdga,
    player_rating
from {{ source('pdga_stg', 'event_details') }}