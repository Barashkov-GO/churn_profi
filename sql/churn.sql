create temp table df as (
select
    rec.client_loyalty_card_number
    ,rec.opened_date::date as dt
    ,lag(rec.opened_dttm) over(partition by rec.client_loyalty_card_number order by rec.opened_dttm)::date as prev_dt
    ,row_number() over (partition by rec.client_loyalty_card_number order by rec.opened_dttm desc) as rn
from dds.v_receipt_lines_sensitive rec
where 1=1
	and rec.line_item_type = 'Normal' --только товары
	and rec.line_quantity > 0
	and rec.line_type in ('Sales','Returns','pickedUp orders') -- исключены авансы
    and rec.client_subtype = 'Professional Card'
);

create temp table cur_month as (
	select
		distinct client_loyalty_card_number
	from dds.v_receipt_lines_sensitive
	where 1=1
		and line_item_type = 'Normal' --только товары
		and line_type in ('Sales','Returns','pickedUp orders') -- исключены авансы
	    and client_subtype = 'Professional Card'    
		and line_quantity > 0
	    and opened_dttm::date between (CURRENT_DATE - INTERVAL '1 month')::date and CURRENT_DATE
);

create temp table main as(
select
	d.client_loyalty_card_number
	,d.dt
	,d.prev_dt
	,coalesce((d.dt - d.prev_dt), -1) as delta
	,cm.client_loyalty_card_number as flag    
from df as d
left join cur_month cm 
on cm.client_loyalty_card_number = d.client_loyalty_card_number
where 
	rn = 1
	and (d.dt - d.prev_dt) between 120 and 360
);

select
    count(case when flag is not null then 1 end) as active
    ,count(case when flag is null then 1 end) as churn
    ,count(client_loyalty_card_number) as cnt
from main;

drop table if exists main;
drop table if exists df;
drop table if exists cur_month;
