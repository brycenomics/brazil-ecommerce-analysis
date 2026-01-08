	本文档用于记录SQL语句的编写思路即具体代码，主要用于数据的初步提取与整理

# 1.月度的销售数据分析: 
使用订单表（olist_orders_dataset）与支付表（olist_order_payments_dataset）做连接，用月度分组来取用每月的销售额，订单量，并做环比分析
```sql
SELECT 
    DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS 年月,
    o.order_status AS 订单状态,
    COUNT(DISTINCT o.order_id) AS 订单数量,
    COUNT(DISTINCT o.customer_id) AS 客户数量,
    SUM(op.payment_value) AS 支付总金额,
    AVG(op.payment_value) AS 平均订单金额,
    LAG(SUM(op.payment_value)) OVER (ORDER BY DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m')) AS 上月支付金额,
    ROUND(
        (SUM(op.payment_value) - LAG(SUM(op.payment_value)) OVER (ORDER BY DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m'))) / 
        NULLIF(LAG(SUM(op.payment_value)) OVER (ORDER BY DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m')), 0) * 100, 
        2
    ) AS 月环比增长率_百分比
FROM olist_orders_dataset o
LEFT JOIN olist_order_payments_dataset op ON o.order_id = op.order_id
WHERE o.order_purchase_timestamp IS NOT NULL
  AND o.order_status = 'delivered'
GROUP BY DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m'), o.order_status
ORDER BY 年月, 订单状态;
```

# 2.支付金额分布分析
 仍旧连接订单表（olist_orders_dataset）与支付表（olist_order_payments_dataset），按每单金额大小分组，每组计算订单总额，并分别按照单数和订单总额计算每组数值在总额（总订单量和总金额）中的占比进行分析。
```sql
WITH order_payments AS (
    SELECT 
        o.order_id,
        o.order_purchase_timestamp,
        SUM(op.payment_value) AS 订单总金额,
        COUNT(op.payment_sequential) AS 支付次数,
        MAX(op.payment_installments) AS 最大分期数
    FROM olist_orders_dataset o
    JOIN olist_order_payments_dataset op ON o.order_id = op.order_id
    WHERE o.order_status = 'delivered'
      AND op.payment_value > 0
    GROUP BY o.order_id, o.order_purchase_timestamp
),
payment_buckets AS (
    SELECT 
        *,
        CASE 
            WHEN 订单总金额 <= 50 THEN '0-50元'
            WHEN 订单总金额 <= 100 THEN '51-100元'
            WHEN 订单总金额 <= 200 THEN '101-200元'
            WHEN 订单总金额 <= 500 THEN '201-500元'
            WHEN 订单总金额 <= 1000 THEN '501-1000元'
            ELSE '1000元以上'
        END AS 金额档位 
    FROM order_payments
)
SELECT 
    金额档位,
    COUNT(*) AS 订单数量,
    SUM(订单总金额) AS 档位总金额,
    AVG(订单总金额) AS 平均订单金额,
    AVG(支付次数) AS 平均支付次数,
    AVG(最大分期数) AS 平均最大分期数,
    ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) AS 订单占比百分比,
    ROUND(SUM(订单总金额) * 100.0 / SUM(SUM(订单总金额)) OVER (), 2) AS 金额占比百分比
FROM payment_buckets
GROUP BY 金额档位
ORDER BY 
    CASE 金额档位
        WHEN '0-50元' THEN 1
        WHEN '51-100元' THEN 2
        WHEN '101-200元' THEN 3
        WHEN '201-500元' THEN 4
        WHEN '501-1000元' THEN 5
        ELSE 6
    END;
```

# 3.RFM用户分层分析
rfm分析对数据抓取要求更高，先对数据进行预处理。
- 利用CTE公用表达式链接订单表与支付表，针对顾客id进行分组，分别统计每个顾客id下最近下单时间（对应R值），下单次数（对应F值）和消费总额（对应M值）。由于数据库的具体聚焦时间为2016到2018年，所以统计最近下单时间（对应R值）时，统一将数据库中所有数据的最后发生日期选定为“今日”，以此为根据计算最近下单时间。

- 随后使用nitile（5）函数将RFM相应数据等分，并赋予对应的5分制的值，实现具体数据的简单数字化，便于分析，此时手中的数据仍旧以用户id（customer_id）为一组数据的唯一识别码，后续的重点将转移到RFM值上
- 根据rmf的值不同，对用户进行分组，并计算每组用户的均销售额，总销售额，均下单次数，均最近下单时间进行整备，作出Excel表格用于分析。
```sql
WITH user_rfm_raw AS (
    SELECT 
        o.customer_id AS 用户ID,
        DATEDIFF(
            (SELECT MAX(order_purchase_timestamp) FROM olist_orders_dataset),
            MAX(o.order_purchase_timestamp)
        ) AS 最近购买天数,
        COUNT(DISTINCT o.order_id) AS 购买次数,
        SUM(op.payment_value) AS 总消费金额
    FROM olist_orders_dataset o
    JOIN olist_order_payments_dataset op ON o.order_id = op.order_id
    WHERE o.order_status = 'delivered'
      AND o.order_purchase_timestamp IS NOT NULL
    GROUP BY o.customer_id
),
rfm_scored AS (
    SELECT 
        *,
        NTILE(5) OVER (ORDER BY 最近购买天数 DESC) AS R得分,
        NTILE(5) OVER (ORDER BY 购买次数) AS F得分,
        NTILE(5) OVER (ORDER BY 总消费金额) AS M得分
    FROM user_rfm_raw
),
user_segments AS (
    SELECT 
        CASE 
            WHEN R得分 >= 4 AND F得分 >= 4 AND M得分 >= 4 THEN '高价值活跃用户'
            WHEN R得分 >= 4 AND M得分 >= 4 THEN '高价值新用户'
            WHEN F得分 >= 4 THEN '频繁购买用户'
            WHEN R得分 <= 2 AND F得分 <= 2 AND M得分 <= 2 THEN '流失风险用户'
            ELSE '一般用户'
        END AS 用户分层,
        COUNT(*) AS 用户数量,
        AVG(最近购买天数) AS 平均最近购买天数,
        AVG(购买次数) AS 平均购买次数,
        AVG(总消费金额) AS 平均消费金额,
        SUM(总消费金额) AS 分层总消费
    FROM rfm_scored
    GROUP BY 
        CASE 
            WHEN R得分 >= 4 AND F得分 >= 4 AND M得分 >= 4 THEN '高价值活跃用户'
            WHEN R得分 >= 4 AND M得分 >= 4 THEN '高价值新用户'
            WHEN F得分 >= 4 THEN '频繁购买用户'
            WHEN R得分 <= 2 AND F得分 <= 2 AND M得分 <= 2 THEN '流失风险用户'
            ELSE '一般用户'
        END
)
SELECT 
    用户分层,
    用户数量,
    平均最近购买天数,
    平均购买次数,
    平均消费金额,
    分层总消费,
    ROUND(用户数量 * 100.0 / SUM(用户数量) OVER (), 2) AS 用户占比百分比,
    ROUND(分层总消费 * 100.0 / SUM(分层总消费) OVER (), 2) AS 消费占比百分比
FROM user_segments
ORDER BY 
    CASE 用户分层
        WHEN '高价值活跃用户' THEN 1
        WHEN '高价值新用户' THEN 2
        WHEN '频繁购买用户' THEN 3
        WHEN '一般用户' THEN 4
        ELSE 5
    END;
```

# 4.商品品类销售趋势分析
首先3表连接，按月统计不同商品品类的销售数据，再根据一行每一个商品品类计算当月销售额和销售额占比，最后选取每月销售额前10的相关数据抓取进入Excel进一步分析
```sql
WITH product_sales AS (
    SELECT 
        DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m') AS 销售月份,
        COALESCE(p.product_category_name, '未分类') AS 商品类别,
        COUNT(DISTINCT oi.order_id) AS 销售订单数,
        SUM(oi.price) AS 销售总额,
        COUNT(*) AS 销售数量,
        AVG(oi.price) AS 平均单价
    FROM olist_order_items_dataset oi
    JOIN olist_orders_dataset o ON oi.order_id = o.order_id
    LEFT JOIN olist_products_dataset p ON oi.product_id = p.product_id
    WHERE o.order_status = 'delivered'
      AND o.order_purchase_timestamp IS NOT NULL
    GROUP BY DATE_FORMAT(o.order_purchase_timestamp, '%Y-%m'), COALESCE(p.product_category_name, '未分类')
),
category_ranks AS (
    SELECT 
        *,
        ROW_NUMBER() OVER (PARTITION BY 销售月份 ORDER BY 销售总额 DESC) AS 月度销售额排名,
        SUM(销售总额) OVER (PARTITION BY 销售月份) AS 月度总销售额
    FROM product_sales
)
SELECT 
    销售月份,
    商品类别,
    销售订单数,
    销售总额,
    销售数量,
    平均单价,
    月度销售额排名,
    ROUND(销售总额 * 100.0 / 月度总销售额, 2) AS 月度销售额占比百分比
FROM category_ranks
WHERE 月度销售额排名 <= 10
ORDER BY 销售月份, 月度销售额排名;
```

```
（以上是我个人对于这一数据库抓取数据的简单思考过程，全部代码附上。）

```
