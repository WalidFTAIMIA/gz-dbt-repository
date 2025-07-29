with sales as (
    select
        *
    from {{ ref('int_orders_margin') }}
),
ship_cost as (
    select
    *
    from {{ ref('stg_gz_raw_data__ship') }}
        
)
select 
    s.orders_id,
    s.date_date,
    (s.margin + sc.shipping_fee - sc.logcost - sc.ship_cost) as Operational_margin

from sales s
left join ship_cost as sc 
    on s.orders_id = sc.orders_id
