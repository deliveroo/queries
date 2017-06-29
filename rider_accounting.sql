GRANT SELECT ON scratch.product_analytics.rider_accounting TO ROLE SYSADMIN
;
drop table if exists scratch.product_analytics.rider_accounting
;
-- ABOUT
-- This script creates a table with one line for every day and rider, starting with the day of the 
-- riders first shift, ending with the current date.
-- each line has various stats about the rider, the facts are grouped by type: acquisition or current.
-- there are also stats grouped by type: work, referral, monthly.
-- the script works by making a sub table for each of these facts and stats and then joining them
-- together at the end


create table SCRATCH.PRODUCT_ANALYTICS.RIDER_accounting
as(
    -- 1. Get acquired facts table
        With drivers_ranked as (
          select 
              driver_id,
              ROW_NUMBER() OVER(PARTITION BY driver_id 
                                 ORDER BY a.date asc) rnk,
              a.zone_code acq_zone,
              a.city_name acq_city,
              a.country_name acq_country,
              a.vehicle_type_name acq_vehicle,
              a.DATE acq_date 
          from PRODUCTION.DENORMALISED.DENORMALISED_DRIVER_HOURS_WORKED a
          WHERE 
          --remove data pre 2011 as incorrect
           date_part('year',a.date)>2010
          order by a.date asc
          )
         -- filter to get the first shift
         ,acquired_facts as (
           select
           *
           from drivers_ranked
           where rnk = 1)
       
        -- 2. Create table with a row for every driver day 
        , all_dates as(SELECT 
              a.DRIVER_ID,
              a.acq_date,
              date.DATE cur_date,
              datediff(day,a.acq_date,date.DATE) days_since_acq
              
          FROM acquired_facts a
          join scratch.static.dates_lookup date 
              on date.DATE >= a.acq_date
              and date.date <= CURRENT_DATE() 
                      )

        -- 3. Make work stats table
          , work_stats as (SELECT 
              a.DRIVER_ID, 
              a.cur_date,
              min(datediff(day,c.date,a.cur_date)) days_since_worked,
              sum(c.hrs_worked_paid) hrs_worked_cumulative,
              sum(c.orders_delivered) orders_cumulative,
              sum(case when c.date=a.cur_date
                   then c.hrs_worked_paid else 0 end) hrs_worked,
              sum(case when c.date=a.cur_date
                   then c.orders_delivered else 0 end) orders,
              case when max(case when c.date=a.cur_date
                   then c.hrs_worked_paid else 0 end) >=20/60 then 1 
                          else 0 end  as active
              
          FROM all_dates a
          left join PRODUCTION.DENORMALISED.DENORMALISED_DRIVER_HOURS_WORKED c 
                on c.date <= a.cur_date
                and c.driver_id = a.driver_id
                and date_part('year',c.date)>2010                                                   
            group by a.cur_date,a.driver_id  
           )
 
       -- 4. Make current facts table (location, vehicle...)
          ,driver_shifts_ranked as (
          select 
              driver_id,
              ROW_NUMBER() OVER(PARTITION BY driver_id,a.date 
                                 ORDER BY a.HRS_WORKED_PAID desc) rnk,
              a.zone_code cur_zone,
              a.city_name cur_city,
              a.country_name cur_country,
              a.vehicle_type_name cur_vehicle,
              a.DATE 
          from PRODUCTION.DENORMALISED.DENORMALISED_DRIVER_HOURS_WORKED a
          WHERE 
          -- remove data pre 2011 as incorrect
           date_part('year',a.date)>2010
          )
  
         -- filter to get the longest shift of the day
         ,driver_longest_shift_of_day as (
           select
           *
           from driver_shifts_ranked
           where rnk = 1)
  
        -- add on to every rider day
          , current_facts_ranked as (
            select 
             a.*,s.cur_zone, s.cur_city, s.cur_country, s.cur_vehicle, s.date
            ,ROW_NUMBER() OVER(PARTITION BY s.driver_id,a.cur_date 
                                 ORDER BY s.date desc) rnk2
            from all_dates a
            join driver_longest_shift_of_day s
                on s.driver_id = a.driver_id
                and s.date <= a.cur_date
            )  
         , current_facts as (
           select
           *
           from current_facts_ranked
           where rnk2 = 1)       
            
       -- 5. Get referral stats     
          ,referrals as (select  
              o.referrer_id driver_id, 
              TO_DATE(o.created_at) date,
              count(distinct applicant_email) referrals_made, 
              count(distinct (case when a.rnk = 1 then applicant_email end)) referred_riders
            from production.onboardiq.OBIQ_EXPORT_CLEAN_INTERNATIONAL o
            left join acquired_facts a 
                on a.driver_id = o.driver_id_mapping 
                and o.most_recent = 'TRUE'
            group by o.referrer_id, TO_DATE(o.created_at)
            )
  
         ,referral_stats as (   
           select 
             a.driver_id
             , a.cur_date
             ,sum(referrals_made) referrals_made_cumulative
             ,sum(referred_riders) referred_riders_cumulative
             ,sum(case when r.date = a.cur_date then referrals_made end) referrals_made
             ,sum(case when r.date = a.cur_date then referred_riders end) referred_riders                   
           from all_dates a
           left join referrals r
            on r.driver_id= a.driver_id
            and r.date <= a.cur_date
           group by a.driver_id, a.cur_date
           )
  
      -- 5. Get monthly stats and flow statuses
         , monthly_stats_temp as (
           select 
            a.driver_id,
           date_trunc('month',a.cur_date) cur_month,
            max(active) as active_month
           
           from work_stats a
           group by a.driver_id,date_trunc('month',a.cur_date)     
          )
  
         , monthly_stats as(
          select
                a.*
                , m2.active_month
                , case 
                    when m2.cur_month = date_trunc('month',a.acq_date) then 'new'
                    when m2.active_month = 0 and m1.active_month =1 then 'churned'
                    when m2.active_month = 1 and m1.active_month =0  then 'resurrected'
                    else 'stable'
                    end as flow_month
          from   all_dates a
          left join monthly_stats_temp m2 
              on m2.driver_id  = a.driver_id
              and m2.cur_month = date_trunc('month',a.cur_date)
           left join monthly_stats_temp m1 
              on m1.driver_id  = a.driver_id
              and m1.cur_month = date_trunc('month',dateadd('month',-1,a.cur_date))
            )
          
        -- 6. Join all the data to all_dates to create the output table
           select 
            a.driver_id
           , a.cur_date           
           , c.cur_vehicle
           , c.cur_country
           , c.cur_city
           , c.cur_zone
           , af.acq_date
           , af.acq_vehicle
           , af.acq_country
           , af.acq_city
           , af.acq_zone
           ,o.channel_hl_code_matched acq_channel
           ,o.channel_detailed_code_matched acq_channel_detailed           
           , a.days_since_acq
           ,floor(a.days_since_acq/7) weeks_since_acq
           ,floor(a.days_since_acq/28) months_since_acq          
           , w.days_since_worked
           , w.hrs_worked
           , w.orders
           , w.hrs_worked_cumulative
           , w.orders_cumulative
           , w.active
           , m.active_month
           , m.flow_month
           , r.referrals_made
           , r.referred_riders
           , r.referrals_made_cumulative
           , r.referred_riders_cumulative

           from all_dates a
           left join monthly_stats m
                on m.driver_id = a.driver_id
                and m.cur_date = a.cur_date
           left join work_stats w
               on w.driver_id = a.driver_id
               and w.cur_date = a.cur_date
           left join referral_stats r
              on r.driver_id = a.driver_id
              and r.cur_date = a.cur_date
           left join production.onboardiq.OBIQ_EXPORT_CLEAN_INTERNATIONAL o
               on o.driver_id_mapping = a.driver_id
               and o.most_recent = 'TRUE'
           left join current_facts c 
                on c.driver_id = a.driver_id
                and c.cur_date = a.cur_date
           left join acquired_facts af
                on af.driver_id = a.driver_id


  
)