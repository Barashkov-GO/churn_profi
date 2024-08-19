----Выгрузка датасета для сегментации
SELECT t4.calc_date,
    t4.pattern,
    t4.is_purch_in_calc_month,
    tc.first_date,
    t4.client_number,
    t4.client_subtype,
    tld.district,
    tld.store_id,
    CASE WHEN t4.margin >= 0.03 THEN 0 ELSE 1 END AS is_margin_stop_segment,
    t4.monetary
FROM
    (
    SELECT t3.calc_date,
        SUM(t3.p_4)::varchar || SUM(t3.p_3)::varchar || SUM(t3.p_2)::varchar || SUM(t3.p_1)::varchar AS pattern,
        SUM(t3.is_purch_in_calc_month) AS is_purch_in_calc_month,
        t3.client_number,
        t3.client_subtype,
        COALESCE(sum(t3.margin) / nullif(sum(t3.wo_nds), 0), 0) AS margin,
        sum(t3.turnover) as monetary
    FROM
        (
        SELECT t2.calc_date,
            CASE WHEN t2."period" = 'p_4' THEN 1 ELSE 0 END AS p_4,
            CASE WHEN t2."period" = 'p_3' THEN 1 ELSE 0 END AS p_3,
            CASE WHEN t2."period" = 'p_2' THEN 1 ELSE 0 END AS p_2,
            CASE WHEN t2."period" = 'p_1' THEN 1 ELSE 0 END AS p_1,
            CASE WHEN t2."period" = 'p_0' THEN 1 ELSE 0 END AS is_purch_in_calc_month,
            t2."period",
            t2.client_number,
            t2.client_subtype,
            sum(t2.margin) as margin,
            sum(t2.wo_nds) as wo_nds,
            sum(t2.turnover) as turnover
        FROM
            (
            SELECT
                t1.calc_date,
                t1."period",
                t1.client_number,
                t1.client_subtype,
                sum(t1.margin) as margin,
                sum(t1.wo_nds) as wo_nds,
                sum(t1.turnover) as turnover
                FROM
                (
                    SELECT
                        ts.calc_date,
                        ts."period",
                        ts.card_no,
                        CASE
                            when max(ts.client_subtype) over(partition by tcn.client_number) = min(ts.client_subtype) over(partition by tcn.client_number)
                            then min(ts.client_subtype) over(partition by tcn.client_number)
                            else 0
                        END as client_subtype,
                        tcn.client_number,
                        ts.margin,
                        ts.wo_nds,
                        ts.turnover
                    FROM temp_sales ts
                    LEFT JOIN temp_client_numbers tcn ON tcn.card_no = ts.card_no
                ) t1
                WHERE t1.client_subtype::integer = 1  -- Оставим только тех клиентов, у которых однозначно определен тип b2c
                GROUP BY
                t1.calc_date,
                t1."period",
                t1.client_subtype,
                t1.client_number
            ) t2
        GROUP BY
            t2.calc_date,
            t2."period",
            t2.client_number,
            t2.client_subtype
        ) t3
    GROUP BY
    t3.calc_date,
    t3.client_number,
    t3.client_subtype
    ) t4
LEFT JOIN temp_cohort tc ON tc.client_number = t4.client_number
LEFT JOIN temp_last_district tld ON tld.client_number = t4.client_number
WHERE (t4.pattern != '0000' OR t4.is_purch_in_calc_month != 0)  -- Исключим клиентов только с возвратами и без покупок
AND t4.client_number is not null
AND tld.district not in ('Алматы', 'Белгород', 'Орел')