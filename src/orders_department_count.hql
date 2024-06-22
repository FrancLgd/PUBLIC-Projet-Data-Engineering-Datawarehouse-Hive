-- Définir ici la commande souhaitée
-- SET hiveconf:order_id=12;

-- Déterminer le nombre d'articles achetés par département
WITH requested_order_products AS (
    SELECT
        *
    FROM
        freshness.order_products
    WHERE
        order_id = ${hiveconf:order_id}
)
SELECT
    rop.order_id,
    dep.department,
    COUNT(rop.product_id) as nb_achats
FROM
    requested_order_products rop
LEFT JOIN
    freshness.products prod ON rop.product_id = prod.product_id
JOIN 
    freshness.departments dep ON prod.department_id = dep.department_id
GROUP BY
    rop.order_id, dep.department, dep.department_id
ORDER BY
    nb_achats DESC;

