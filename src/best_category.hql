-- Définir ici l'identifiant de l'utilisateur souhaité
-- SET hiveconf:user_id=10;

-- Récupérer le nom de la catégorie où il y a eu le plus d'achats parmi toutes les commandes passées
WITH user_orders AS (
    SELECT 
        *
    FROM 
        freshness.orders
    WHERE
        user_id = ${hiveconf:user_id}
)
SELECT
    user_id,
    al.aisle,
    COUNT(op.product_id) as nb_achats
FROM 
    user_orders uo
LEFT JOIN
    freshness.order_products op ON op.order_id = uo.order_id
JOIN
    freshness.products prod ON op.product_id = prod.product_id
JOIN 
    freshness.aisles al ON prod.aisle_id = al.aisle_id
GROUP BY
    user_id, al.aisle_id, aisle
ORDER BY
    nb_achats DESC
LIMIT 1;