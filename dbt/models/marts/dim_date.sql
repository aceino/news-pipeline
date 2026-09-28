with date_spine as ( 
   select
        unnest(
            generate_series(
                date '2026-01-01',
                date '2026-12-31',
                interval '1 day'
            )
        )::date as full_date
)

select 
    year(full_date) * 10000
        + month(full_date) * 100 
        + day(full_date) as date_key ,

    full_date, 
    year(full_date) as year, 
    quarter(full_date) as quarter, 
    month(full_date) as month, 
    week(full_date) as week, 
    day(full_date) as day,
    monthname(full_date) as month_name,
    dayname(full_date) as day_name

from date_spine 
order by full_date