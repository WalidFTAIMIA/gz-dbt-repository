WITH orders_per_day AS (
   SELECT
     date_date
     ,COUNT(DISTINCT orders_id) AS nb_transactions
     ,ROUND(SUM(revenue),0) AS revenue
     ,ROUND(SUM(margin),0) AS margin
     ,ROUND(SUM(operational_margin),0) AS operational_margin
     ,ROUND(SUM(purchase_cost),0) AS purchase_cost
     ,ROUND(SUM(shipping_fee),0) AS shipping_fee
     ,ROUND(SUM(logcost),0) AS logcost
     ,ROUND(SUM(ship_cost),0) AS ship_cost
     ,SUM(quantity) AS quantity
 FROM {{ref("int_orders_operational")}}
 GROUP BY  date_date
 )

 SELECT
     date_date
     , revenue
     , margin
     , operational_margin
     , purchase_cost
     , shipping_fee
     , logcost
     , ship_cost
     , quantity
     , ROUND(revenue/NULLIF(nb_transactions, 0), 2) AS average_basket
 FROM orders_per_day
 ORDER BY  date_date DESC