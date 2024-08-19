set gp_workfile_limit_per_query='10000MB';

CREATE TEMP TABLE temp_sales
(max_purch_date date,
calc_date date,
"period" varchar(10),
card_no varchar(36),
client_subtype smallint,
district varchar(36),
store_id smallint,
margin numeric,
wo_nds numeric,
turnover numeric)
distributed BY (card_no);

CREATE TEMP TABLE temp_client_numbers
(client_number varchar(36),
card_no varchar(36))
distributed BY (card_no);

CREATE TEMP TABLE temp_cohort
(client_number varchar(36),
first_date date)
distributed BY (client_number);

CREATE TEMP TABLE temp_last_district
(client_number varchar(36),
district varchar(36),
store_id smallint)
distributed BY (client_number);

----Выгрузка данных по продажам
INSERT INTO temp_sales(max_purch_date
    , calc_date
    , "period"
    , card_no
    , client_subtype
    , district
    , store_id
    , margin
    , wo_nds
    , turnover)
SELECT max(t1.purch_date) as max_purch_date
    , t1.calc_date
    , t1."period"
    , t1.card_no
    , max(t1.client_subtype) as client_subtype
    , t1.district
    , t1.store_id
    , sum(CASE 
            WHEN t1.line_item_type='Normal' 
            THEN t1.line_margin 
            ELSE 0 
        END) AS margin  --маржа, руб, с учетом возвратов, без услуг
    , sum(CASE 
            WHEN t1.line_item_type='Normal' 
            THEN t1.line_turnover_wo_vat 
            ELSE 0 
        END) AS wo_nds  --ТО без НДС с учетом возвратов, без услуг 
    , sum(CASE 
            WHEN t1.line_item_type='Normal' 
            THEN t1.line_turnover 
            ELSE 0 
        END) AS turnover  --ТО с учетом возвратов, без услуг
FROM (
    SELECT
        rec.opened_date::date AS purch_date
        ,'{calculation_date}'::date as calc_date
        ,CASE
            WHEN '{calculation_date}'::date - rec.opened_date::date between 0 and 30  -- 1 месяц до расчета
                AND coalesce(rec.line_quantity,0) > 0
                THEN 'p_0'
            WHEN '{calculation_date}'::date - rec.opened_date::date between 31 and 121  -- 2-4 месяца до расчета
                AND coalesce(rec.line_quantity,0) > 0
                THEN 'p_1'
            WHEN '{calculation_date}'::date - rec.opened_date::date between 122 and 213  -- 5-7 месяцев до расчета
                AND coalesce(rec.line_quantity,0) > 0
                THEN 'p_2'
            WHEN '{calculation_date}'::date - rec.opened_date::date between 214 and 304  -- 8-10 месяцев до расчета
                AND coalesce(rec.line_quantity,0) > 0
                THEN 'p_3'
            WHEN '{calculation_date}'::date - rec.opened_date::date between 305 and 397  -- 11-13 месяцев до расчета
                AND coalesce(rec.line_quantity,0) > 0
                THEN 'p_4'
            ELSE 'wo_purch'
        END as "period"  --Разметка периодов внутри окна, например 'p_1' - это 1 квартал назад от месяца расчета
        ,rec.client_loyalty_card_number as card_no
        ,CASE 
            WHEN rec.client_subtype='Service Card' THEN 1
            WHEN rec.client_subtype='Professional Card' THEN 2
            ELSE 0 
        END as client_subtype
        ,vds.district_name as district
        ,rec.store_id::integer
        ,COALESCE(rec.line_margin,0) AS line_margin                 -- чтобы найти % маржи: sum(margin)/sum(wo_nds)
        ,COALESCE(rec.line_total_sum_after_pricecorrections_wo_vat,0) AS line_turnover_wo_vat
        ,COALESCE(rec.line_turnover, 0) AS line_turnover
        ,rec.line_item_type
    FROM  
        dds.v_receipt_lines_sensitive rec
    LEFT JOIN dds.v_dict_stores vds ON vds.store = rec.store_id 
    WHERE 1=1
        and rec.line_type IN ('Sales', 'pickedUp orders', 'Returns')
        AND length(rec.client_loyalty_card_number) > 0
        AND rec.opened_date >= ('{calculation_date}'::date - '13 month'::interval)::date -- calc_date - 13 месяцев
        AND rec.opened_date < '{calculation_date}'::date -- calc_date
        AND rec.client_subtype in ('Professional Card')
) t1
group by 
    t1.calc_date
    ,t1."period"
    ,t1.card_no
    ,t1.district
    ,t1.store_id;

----Получение client_numbers из LYS по номеру сервисной карты
INSERT INTO temp_client_numbers(client_number
    , card_no)
select t3.client_number
    , COALESCE(t2.card_no, t3.client_number) as card_no
from     
    (
    select t1.client_number
        , t1.card_no      
    from 
        (
        select vs2.customer::varchar as client_number
            , vs2."number"::varchar as card_no
            , ROW_NUMBER() OVER(
                PARTITION BY vs2."number"::varchar
                ORDER BY vs2.validitystartdate desc, vs2.updated_dttm desc) AS rn 
        from lysloyalty_ods.v_account va2  
        inner join ( 
            SELECT * 
            FROM lysloyalty_ods.v_support
            UNION ALL 
            SELECT *
            FROM lysloyalty_ods.v_cleared_support) vs2
        on va2.id = vs2.accountid
        WHERE 1=1
        and vs2."number" is not null
        AND vs2."number" NOT LIKE (vs2.customer || '%') --эти карты не используются, но они заведены
        ) t1
        WHERE t1.rn = 1
    ) t2
    RIGHT JOIN (
        select distinct COALESCE(subq2.client_number, ts.card_no) as client_number
        from 
            (
            select subq.client_number
                , subq.card_no
            from
                (
                select vs.customer::varchar as client_number
                    , vs."number"::varchar as card_no
                    , ROW_NUMBER() OVER(
                        PARTITION BY vs."number"::varchar
                        ORDER BY vs.validitystartdate desc, vs.updated_dttm desc) AS rn 
                from lysloyalty_ods.v_account va
                inner join ( 
                    SELECT * 
                    FROM lysloyalty_ods.v_support
                    UNION ALL 
                    SELECT *
                    FROM lysloyalty_ods.v_cleared_support vcs1) vs
                on va.id = vs.accountid
                WHERE 1=1
                and vs."number" is not null
                AND vs."number" NOT LIKE (vs.customer || '%') --эти карты не используются, но они заведены
                ) subq
                WHERE subq.rn = 1
            ) subq2
            right join (select distinct card_no from temp_sales) ts
            on ts.card_no = subq2.card_no
    ) t3
    ON t3.client_number = t2.client_number;

----Выгрузка первой даты покупки по card_no
INSERT INTO temp_cohort(client_number
    , first_date)
SELECT t1.client_number
    , t1.opened_date::date as first_date
FROM (
    SELECT rec.client_loyalty_card_number as card_no
        , tcn.client_number
        , rec.opened_date
        , row_number() over(
            partition by tcn.client_number 
            order by rec.opened_date asc) as rn
    FROM dds.v_receipt_lines_sensitive rec
    INNER JOIN temp_client_numbers tcn on tcn.card_no = rec.client_loyalty_card_number
    where 1=1
    and rec.line_type in ('Sales', 'pickedUp orders')
    and rec.line_quantity > 0                                       --- ONLY fact OF sales wo Returns
    and length(rec.client_loyalty_card_number) > 0
    and rec.client_subtype in ('Professional Card')
    AND rec.opened_date BETWEEN '2016-10-01'::date AND '2099-12-31'::date
) t1
where t1.rn = 1;

----Выгрузка города и магазина, в котором клиент покупал последний раз
INSERT INTO temp_last_district (client_number
    , district
    , store_id)
SELECT client_number
    , district
    , store_id
FROM(
     SELECT tcn.client_number
         , ts.district
         , ts.store_id
         , ROW_NUMBER() OVER(
             PARTITION BY tcn.client_number
             ORDER BY ts.max_purch_date DESC) AS rn  -- чтобы выбрать последнюю дату покупки
     FROM temp_sales ts
     LEFT JOIN temp_client_numbers tcn on tcn.card_no = ts.card_no
    ) t
WHERE rn = 1;