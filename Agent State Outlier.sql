select
  a.date as ind_date,
  a.agent as ind_agent,
  d.agent_group as ind_agent_group,
  d.agent_start_date as ind_agent_start_date,

  e."total_talk_time_seconds" as ind_total_talk_time_seconds,
  e."all_call_count" as ind_call_count,
  round((((e."total_talk_time_seconds" + a."After Call Work Sum" )/60.00) / e."all_call_count"),2) as average_ind_handle_time_minutes,
  percent_rank() over (partition by a.date order by average_ind_handle_time_minutes asc) as handle_time_flag,

  a."After Call Work Sum" as ind_after_Call_work_sum,
  a."Login Sum" as ind_login_sum,
  a."Break Sum" as ind_break_sum,
  a."Follow-Up Work Sum" as ind_follow_up_sum,
  a."Personal Sum" as ind_personal_sum,
  a."Hold Sum" as ind_hold_sum,
  a."Ready Sum" as ind_ready_sum,

  a."After Call Percentage" as ind_after_call_percent,
  a."Break Percentage" as ind_break_percent,
  a."Follow-Up Percentage" as ind_follow_up_percent,
  a."Personal Percentage" as ind_personal_percent,
  a."Hold Percentage" as ind_hold_percent,

  c."After Call Average" as group_after_call_average,
  c."Break Average" as group_break_average,
  c."Follow-Up Average" as group_follow_up_average,
  c."Personal Average" as group_personal_average,
  c."Hold Average" as group_hold_average,

  c."After Call SD" as group_after_call_dev,
  c."Break SD" as group_break_dev,
  c."Follow-Up SD" as group_follow_up_dev,
  c."Personal SD" as group_personal_dev,
  c."Hold SD" as group_hold_dev

from(

---Individual level aggregation--
select
  date,
  agent,
  sum("'After Call Work '") as "After Call Work Sum",
  sum("'Login '") as "Login Sum",
  sum("'Not Ready Break'") as "Break Sum",
  sum("'Not Ready Follow-Up Work'") as "Follow-Up Work Sum",
  sum("'Not Ready Personal'") as "Personal Sum",
  sum("'On Hold '") as "Hold Sum",
  sum("'Ready '") as "Ready Sum",

  "After Call Work Sum"/NULLIF("Login Sum", 0) * 100.00 as "After Call Percentage",
  "Break Sum"/NULLIF("Login Sum", 0) * 100.00 as "Break Percentage",
  "Follow-Up Work Sum"/NULLIF("Login Sum", 0) * 100.00 as "Follow-Up Percentage",
  "Personal Sum"/NULLIF("Login Sum", 0) * 100.00 as "Personal Percentage",
  "Hold Sum"/NULLIF("Login Sum", 0) * 100.00 as "Hold Percentage"
from (

select
  state,
  reason_code,
  CONCAT(state, ' ' , IFNULL (REASON_CODE , ' ' )) AS cat,
  AGENT_STATE_TIME_SECONDS,
  date,
  agent,
  agent_group
from PRODUCTION.SOURCE_FIVE9.AGENT_STATE_LOGS)

pivot(sum(AGENT_STATE_TIME_SECONDS)
  for cat in (
  'After Call Work ', 'Login ', 'Ready ', 'On Hold ', 'Not Ready Break', 'Not Ready Follow-Up Work', 'Not Ready Personal'))

where
  agent_group in (
  'PP-CustomerCare-CCSLC01','PP-CustomerCare-CCSLC02','PP-CustomerCare-CCSLC03','PP-CustomerCare-CCSLC04',
  'PP-CustomerCare-CCSLC05','PP-CustomerCare-CCSLC06','PP-CustomerCare-CCSLC07','PP-CustomerCare-CCSLC08',
  'PP-CustomerCare-CCSLC09','PP-CustomerCare-CCSLC10','PP-CustomerCare-CCSLC11')

group by date, agent
order by date desc, agent asc

) as a

-- group level aggregation --
left join (

select
  b.date,
  avg(b."After Call Percentage") as "After Call Average",
  avg(b."Break Percentage") as "Break Average",
  avg(b."Follow-Up Percentage") as "Follow-Up Average",
  avg(b."Personal Percentage") as "Personal Average",
  avg(b."Hold Percentage") as "Hold Average",
  STDDEV_POP(b."After Call Percentage") as "After Call SD",
  STDDEV_POP(b."Break Percentage") as "Break SD",
  STDDEV_POP(b."Follow-Up Percentage") as "Follow-Up SD",
  STDDEV_POP(b."Personal Percentage") as "Personal SD",
  STDDEV_POP(b."Hold Percentage") as "Hold SD"
from (
select
  date,
  agent,
  sum("'After Call Work '") as "After Call Work Sum",
  sum("'Login '") as "Login Sum",
  sum("'Not Ready Break'") as "Break Sum",
  sum("'Not Ready Follow-Up Work'") as "Follow-Up Work Sum",
  sum("'Not Ready Personal'") as "Personal Sum",
  sum("'On Hold '") as "Hold Sum",
  sum("'Ready '") as "Ready Sum",

  "After Call Work Sum"/NULLIF("Login Sum", 0) * 100.00 as "After Call Percentage",
  "Break Sum"/NULLIF("Login Sum", 0) * 100.00 as "Break Percentage",
  "Follow-Up Work Sum"/NULLIF("Login Sum", 0) * 100.00 as "Follow-Up Percentage",
  "Personal Sum"/NULLIF("Login Sum", 0) * 100.00 as "Personal Percentage",
  "Hold Sum"/NULLIF("Login Sum", 0) * 100.00 as "Hold Percentage"

from (

select
  state,
  reason_code,
  CONCAT(state, ' ' , IFNULL (REASON_CODE , ' ' )) AS cat,
  AGENT_STATE_TIME_SECONDS,
  date,
  agent,
  agent_group
from PRODUCTION.SOURCE_FIVE9.AGENT_STATE_LOGS)

pivot(sum(AGENT_STATE_TIME_SECONDS)
  for cat in (
  'After Call Work ', 'Login ', 'Ready ', 'On Hold ', 'Not Ready Break', 'Not Ready Follow-Up Work', 'Not Ready Personal'))

where
  agent_group in (
  'PP-CustomerCare-CCSLC01','PP-CustomerCare-CCSLC02','PP-CustomerCare-CCSLC03','PP-CustomerCare-CCSLC04',
  'PP-CustomerCare-CCSLC05','PP-CustomerCare-CCSLC06','PP-CustomerCare-CCSLC07','PP-CustomerCare-CCSLC08',
  'PP-CustomerCare-CCSLC09','PP-CustomerCare-CCSLC10','PP-CustomerCare-CCSLC11')

group by date, agent
) as b

group by b.date

) as c

on a.date = c.date

left join PRODUCTION.SOURCE_FIVE9.AGENT_STATE_LOGS as d
on a.date = d.date and a.agent = d.agent

left join (
  select date, agent, sum(talk_time_seconds) as "total_talk_time_seconds", count(distinct call_id) as "all_call_count"
  from PRODUCTION.SOURCE_FIVE9.AGENT_STATE_LOGS
  where call_type in ('Outbound', 'Inbound', 'Manual', 'Queue Callback', 'Preview', 'Skill call')
  group by 1,2) as e
on a.date = e.date and a.agent = e.agent

group by
ind_date,
ind_agent,
ind_agent_group,
ind_agent_start_date,
ind_call_count,
ind_total_talk_time_seconds,
ind_after_Call_work_sum,
ind_login_sum,
ind_break_sum,
ind_follow_up_sum,
ind_personal_sum,
ind_hold_sum,
ind_ready_sum,
ind_after_call_percent,
ind_break_percent,
ind_follow_up_percent,
ind_personal_percent,
ind_hold_percent,
group_after_call_average,
group_break_average,
group_follow_up_average,
group_personal_average,
group_hold_average,
group_after_call_dev,
group_break_dev,
group_follow_up_dev,
group_personal_dev,
group_hold_dev

order by a.date desc--, a.agent asc
