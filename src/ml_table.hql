SELECT
    -- Identifiant de la commande, du produit et de la catégorie
    op.order_id,
    op.product_id,
    prod.aisle_id,

    -- Nom du produit et de la catégorie
    prod.product_name,
    ai.aisle,

    -- Informations temporelles connues concernant les articles ou les commandes
    op.add_to_cart_order,
    ord.days_since_prior_order,
    ord.order_hour_of_day,

    -- Informations sur l'utilisateur
    ord.user_id,
    opu.avg_products_per_order
FROM
    freshness.order_products op
LEFT JOIN
    freshness.products prod ON op.product_id = prod.product_id
JOIN
    freshness.aisles ai ON ai.aisle_id = prod.aisle_id
JOIN
    freshness.orders ord ON ord.order_id = op.order_id
JOIN
    freshness.orders_per_users opu ON opu.user_id = ord.user_id;