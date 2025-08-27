with b as (
  select
    bet_id,
    player_id,
    game_id,
    placed_at,                         -- was bet_time
    stake::double precision   as stake,
    odds::double precision    as odds,
    status,
    actual_win::double precision as actual_win
  from stg_bets
)

select
  bet_id,
  player_id,
  game_id,
  cast(placed_at as date) as bet_date, -- was bet_time
  placed_at,
  stake,
  odds,
  status,
  actual_win
from b
