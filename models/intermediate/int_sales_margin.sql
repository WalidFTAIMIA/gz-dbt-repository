with sales as (
    select
      product_id,
      quantity,
      revenue
    from {{ ref('stg_gz_raw_data__sales') }}
),

products as (
    select
      products_id,
      cast(purchase_price as float64) as purchase_price
    from {{ ref('stg_gz_raw_data__product') }}
)

select
  s.product_id,
  s.quantity,
  s.revenue,
  
  -- 1) purchase_cost = qty * purchase_price
  s.quantity * p.purchase_price  as purchase_cost,
  
  -- 2) margin = revenue - purchase_cost
  s.revenue - (s.quantity * p.purchase_price)  as margin

from sales s

left join products p
  on s.product_id = p.products_id