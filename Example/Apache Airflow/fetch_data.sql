-- fetch_data.sql

SELECT *
FROM 
    Order
JOIN 
    Order_Detail ON Order.OrderID = Order_Detail.OrderID
WHERE 
    Order.OrderDate >= current_date - interval '7 days';


-- SELECT 
--     COUNT(DISTINCT o.order_id) AS order_volume,
--     AVG(o.total_amount) AS average_order_value,
--     AVG(basket_size) AS basket_size
-- FROM 
--     Order o
-- JOIN 
--     (
--         SELECT 
--             od.order_id, 
--             SUM(od.product_id*od.quantity) AS basket_size
--         FROM 
--             Order_Detail od
--         JOIN 
--             Order o ON od.order_id = o.order_id
--         WHERE 
--             od.order_date >= current_date - interval '7 days'
--         GROUP BY 
--             od.order_id
--     ) od ON o.order_id = od.order_id;
