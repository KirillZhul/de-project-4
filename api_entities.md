**Описание задачи**

Вам необходимо построить витрину, содержащую информацию о выплатах курьерам.
Состав витрины:

- **id** — идентификатор записи.  
- **courier_id** — ID курьера, которому перечисляем.  
- **courier_name** — Ф. И. О. курьера.  
- **settlement_year** — год отчёта.  
- **settlement_month** — месяц отчёта, где 1 — январь и 12 — декабрь.  
- **orders_count** — количество заказов за период (месяц).
- **orders_total_sum** — общая стоимость заказов.
- **rate_avg** — средний рейтинг курьера по оценкам пользователей.  
- **order_processing_fee** — сумма, удержанная компанией за обработку заказов, которая высчитывается как orders_total_sum * 0.25.  
- **courier_order_sum** — сумма, которую необходимо перечислить курьеру за доставленные им/ей заказы. За каждый доставленный заказ курьер должен получить некоторую сумму в зависимости от рейтинга (см. ниже).  
- **courier_tips_sum** — сумма, которую пользователи оставили курьеру в качестве чаевых.  
- **courier_reward_sum** — сумма, которую необходимо перечислить курьеру. Вычисляется как courier_order_sum + courier_tips_sum * 0.95 (5% — комиссия за обработку платежа).  

Правила расчёта процента выплаты курьеру в зависимости от рейтинга, где r — это средний рейтинг курьера в расчётном месяце:

    r < 4 — 5% от заказа, но не менее 100 р.;
    4 <= r < 4.5 — 7% от заказа, но не менее 150 р.;
    4.5 <= r < 4.9 — 8% от заказа, но не менее 175 р.;
    4.9 <= r — 10% от заказа, но не менее 200 р.
	
Так, как таблица находится в CDM-слое, данные в неё будут загружаться из двух таблиц в DDS-слое (имя курьера из **dm_couriers** и данные о доставке из **fct_deliveries**), а сами эти две таблицы будут набирать данные из STG-слоя. 

    DDL таблицы cdm.dm_courier_ledger:
	
CREATE TABLE if not exists cdm.dm_courier_ledger (  
id serial NOT NULL,  
courier_id varchar NOT null,  
courier_name varchar NOT NULL,  
   settlement_year int4 NOT NULL,  
   settlement_month int4 NOT NULL,
   orders_count int4 NOT NULL,
   orders_total_sum numeric(19, 5) NOT NULL,  
   rate_avg numeric(2, 1) NOT NULL,  
   order_processing_fee numeric(19, 5) NOT NULL,  
   courier_order_sum numeric(19, 5) NOT NULL,  
   courier_tips_sum numeric(19, 5) NOT NULL,  
   courier_reward_sum numeric(19, 5) NOT NULL,  
   CONSTRAINT dm_courier_ledger_settlement_year_check CHECK (settlement_year > 0),  
   CONSTRAINT dm_courier_ledger_settlement_month_check CHECK (settlement_month > 0),  
   CONSTRAINT dm_courier_ledger_orders_count_check CHECK (orders_count >= 0),  
   CONSTRAINT dm_courier_ledger_orders_total_sum_check CHECK (orders_total_sum >= (0)::numeric),  
   CONSTRAINT dm_courier_ledger_rate_avg_check CHECK (rate_avg >= (0)::numeric),  
   CONSTRAINT dm_courier_ledger_order_processing_fee_check CHECK (order_processing_fee >= (0)::numeric),  
   CONSTRAINT dm_courier_ledger_order_courier_order_sum_check CHECK (courier_order_sum >= (0)::numeric),  
   CONSTRAINT dm_courier_ledger_order_courier_tips_sum_check CHECK (courier_tips_sum >= (0)::numeric),  
   CONSTRAINT dm_courier_ledger_order_courier_reward_sum_check CHECK (courier_reward_sum >= (0)::numeric),  
   CONSTRAINT pk_dm_courier_ledger PRIMARY KEY (id),  
   CONSTRAINT unique_dm_courier_ledger UNIQUE (courier_id, settlement_year, settlement_month)  
);  

	DDL таблицы dds.dm_couriers:
		
CREATE TABLE dds.dm_couriers (  
   id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),  
   courier_id varchar NOT NULL,  
   courier_name varchar NOT NULL,  
   active_from timestamp NOT null,  
   active_to timestamp NOT null,  
   CONSTRAINT dm_couriers_courier_id_key UNIQUE (courier_id),  
   CONSTRAINT dm_couriers_pkey PRIMARY KEY (id)  
);  

        DDL таблицы dds.fct_deliveries:

CREATE TABLE dds.fct_deliveries (  
   id int4 NOT NULL GENERATED ALWAYS AS IDENTITY( INCREMENT BY 1 MINVALUE 1 MAXVALUE 2147483647 START 1 CACHE 1 NO CYCLE),  
   delivery_id varchar NOT NULL,  
   courier_id int4 NOT NULL,  
   order_id varchar NOT NULL,  
   delivery_ts timestamp not null,  
   rate int4 NOT NULL,  
   sum numeric(19, 5) NOT NULL DEFAULT 0,  
   tip_sum numeric(19, 5) NOT NULL DEFAULT 0,  
   CONSTRAINT fct_deliveries_delivery_id_key UNIQUE (delivery_id),  
   CONSTRAINT fct_deliveries_pkey PRIMARY KEY (id),  
   CONSTRAINT fct_deliveries_rate_check CHECK ((rate > 0))  
);  


        DDL таблицы stg.couriers:
	
CREATE TABLE stg.couriers (  
   id serial4 NOT NULL,  
   object_value text NOT NULL,  
   update_ts timestamp NOT NULL,  
   CONSTRAINT couriers_pkey PRIMARY KEY (id)  
);  


       DDL таблицы stg.delivery:
	
CREATE TABLE stg.delivery (  
  id serial4 NOT NULL,  
  object_value text NOT NULL,  
  update_ts timestamp NOT NULL,  
  CONSTRAINT delivery_pkey PRIMARY KEY (id)  
);  


Для реализации финального расчёт с курьерами мы будем использовать следующую структуру запроса внесения изменения в таблицу cdm.dm_courier_ledger:

insert into cdm.dm_settlement_report (  
restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum,   orders_bonus_granted_sum,   order_processing_fee, restaurant_reward_sum)    
select  r.id as restaurant_id,     
r.restaurant_name,   
d.date as settlement_date,   
count(distinct s.order_id) as orders_count,   
sum(s.total_sum) as orders_total_sum,   
sum(s.bonus_payment) as orders_bonus_payment_sum,   
sum(s.bonus_grant) as orders_bonus_granted_sum,   
                                sum(s.total_sum * 0.25) as order_processing_fee,   
                                sum(s.total_sum - s.total_sum * 0.25 - s.bonus_payment) as restaurant_reward_sum  
                        from dds.dm_restaurants r   
                            inner join dds.dm_orders o on r.id = o.restaurant_id   
                            inner join dds.fct_product_sales s on s.order_id = o.id   
                            inner join dds.dm_timestamps d on d.id = o.timestamp_id   
                        where r.active_to > now() and o.order_status = 'CLOSED'  
                        group by  r.id, r.restaurant_name, d.date  
                        ON CONFLICT (restaurant_id, settlement_date)   
                        DO UPDATE   
                        SET  
                            orders_count = EXCLUDED.orders_count,  
                            orders_total_sum = EXCLUDED.orders_total_sum,  
                            orders_bonus_payment_sum = EXCLUDED.orders_bonus_payment_sum,  
                            orders_bonus_granted_sum = EXCLUDED.orders_bonus_granted_sum,  
                            order_processing_fee = EXCLUDED.order_processing_fee,  
                            restaurant_reward_sum = EXCLUDED.restaurant_reward_sum;  

DELETE FROM cdm.dm_courier_ledger;  

WITH over_count AS(  
select  
	order_ts,  
	courier_id,   
	count(*) over (partition by courier_id) as orders_count,  
	sum(sum) over (partition by courier_id) as orders_total_sum,  
	avg(rate) over (partition by courier_id) as rate_avg,  
	sum(tip_sum) over (partition by courier_id) as courier_tips_sum  
from dds.fct_deliveries  
), order_sum as   
(  
select order_ts,  
	courier_id,  
	CASE   
		WHEN rate_avg < 4 THEN (orders_total_sum * 0.05)  
		WHEN 4 <= rate_avg and rate_avg < 4.5 THEN (orders_total_sum * 0.07)  
		WHEN 4.5 <= rate_avg and rate_avg < 4.9 THEN (orders_total_sum * 0.08)  
		WHEN 4.9 < rate_avg THEN (orders_total_sum * 0.1)  
	end courier_order  
from over_count  
), order_sum2 as   
(  
select order_ts,  
	courier_id,  
	CASE   
		WHEN courier_order < 100 THEN 100  
		else courier_order  
	end courier_order_sum  
from order_sum  
)  
insert into   cdm.dm_courier_ledger(courier_id,courier_name,settlement_year,settlement_month,orders_count,orders_total_sum,rate_avg,order_processing_fee,courier_order_sum,courier_tips_sum,courier_reward_sum)  
select distinct  
	dc.courier_id,  
	dc.courier_name,  
	EXTRACT(YEAR FROM fd.order_ts) settlement_year,  
	EXTRACT(month FROM fd.order_ts) settlement_month,  
	oc.orders_count,  
	oc.orders_total_sum,  
	oc.rate_avg,  
	(oc.orders_total_sum * 0.25) order_processing_fee,  
	os2.courier_order_sum,  
	oc.courier_tips_sum,  
	((os2.courier_order_sum + oc.courier_tips_sum)*0.95) courier_reward_sum  
from dds.fct_deliveries fd  
LEFT join dds.dm_couriers dc on fd.courier_id = dc.courier_id  
left join over_count oc on fd.courier_id = oc.courier_id and fd.order_ts = oc.order_ts  
left join order_sum2 os2 on fd.courier_id = os2.courier_id and fd.order_ts = os2.order_ts  
group by dc.courier_id, dc.courier_name, fd.order_ts, oc.orders_count, oc.orders_total_sum, oc.rate_avg,
		os2.courier_order_sum, oc.courier_tips_sum  
;
 


