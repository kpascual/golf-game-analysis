{{ config(materialized = 'table') }}

SELECT
    practice_date,
    practice_date_index,
    club,
    club_speed,
    ball_speed
FROM
    shots
WHERE
    club = 'Driver'
