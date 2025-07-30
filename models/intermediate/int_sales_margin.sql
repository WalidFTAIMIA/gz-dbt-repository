with
-- 1) On ne garde que les enregistrements valides
sales_raw as (
  select
    orders_id,
    product_id,                 -- <--- ici
    date_date,
    cast(quantity   as numeric) as quantity,
    cast(revenue   as numeric) as revenue
  from {{ ref('stg_gz_raw_data__sales') }}
  where product_id is not null -- <--- et ici
),

-- 2) On prépare la table des prix d’achat
product_price as (
  select
    products_id,                
    cast(purchase_price as numeric) as purchase_price
  from {{ ref('stg_gz_raw_data__product') }}
),

-- 3) On regroupe pour n’avoir qu’une ligne par (order, product)
dedup as (
  select
    s.orders_id,
    s.product_id,               -- <--- ici aussi
    min(s.date_date)                     as date_date,
    sum(s.quantity)                      as quantity,
    sum(s.revenue)                       as revenue,
    sum(s.quantity * p.purchase_price)   as purchase_cost
  from sales_raw s
  left join product_price p
    on s.product_id = p.products_id      
  group by
    s.orders_id,
    s.product_id
)

-- 4) Enfin, on calcule la marge
select
  orders_id,
  product_id   as products_id,           
  date_date,
  quantity,
  revenue,
  purchase_cost,
  revenue - purchase_cost as margin
from dedup
