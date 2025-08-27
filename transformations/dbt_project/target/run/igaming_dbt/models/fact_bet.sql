
  create view "igaming"."public"."fact_bet__dbt_tmp"
    
    
  as (
    with b as (
  select * from "igaming"."public"."stg_bets"
)
select
  bet_id,
  player_id,
  game_id,
  stake,
  odds,
  (stake * (odds - 1)) as potential_win,
  actual_win,
  cast(bet_time as date) as bet_date
from b
  );