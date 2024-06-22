-- Définir ici le nom du produit souhaité
-- SET hiveconf:product_name='Honey Mustard';

-- Déterminer l'heure à laquelle l'article a été le plus acheté
WITH requested_product AS (
    SELECT 
        *
    FROM 
        freshness.products
    WHERE
        product_name = ${hiveconf:product_name}
)
SELECT
    rp.product_id,
    rp.product_name,
    o.order_hour_of_day,
    COUNT(o.order_id) as nb_achats
FROM 
    requested_product rp
LEFT JOIN
    freshness.order_products op ON rp.product_id = op.product_id
JOIN 
    freshness.orders o ON op.order_id = o.order_id
GROUP BY
    rp.product_id, rp.product_name, o.order_hour_of_day
ORDER BY
    nb_achats DESC
LIMIT 1;