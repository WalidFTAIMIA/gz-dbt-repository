with sales as (
    select
        *
    from {{ ref('int_orders_margin') }}
)
,
ship  as (
    select 
        *
    from {{ ref('stg_gz_raw_data__ship') }}
)
,
purchase_cost as (
    select 
        *
    from {{ ref('int_sales_margin') }}
)
,
Operational_margin as (
    select
        *
    from {{ ref('int_orders_operational') }}
)
select 
    s.date_date as date,
    count(DISTINCT s.orders_id) as total_transactions,
    sum(s.revenue) as total_revenue,
    round(safe_divide(sum(s.revenue), count(DISTINCT s.orders_id)),2) as average_basket,
    sum(op.operational_margin) as total_operational_margin,
    sum(pc.purchase_cost) as total_purchase_cost,
    sum(sh.shipping_fee) as total_shipping_fees,
    sum(sh.logcost) as total_logs_costs,
    sum(s.quantity) as total_quantity_sold
from sales as s
left join ship sh on s.orders_id = sh.orders_id
left join purchase_cost pc on s.orders_id = pc.orders_id
left join Operational_margin op on s.orders_id = op.orders_id
group by 1
order by 1