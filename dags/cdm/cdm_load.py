import os
from logging import Logger
from pathlib import Path

from lib import PgConnect


class MartLoader:
    def __init__(self, pg: PgConnect) -> None:
        self._db = pg
    
    def load_cdm(self) -> None:
        with self._db.connection() as conn:
            with conn.cursor() as cur:
                    cur.execute(
                        """
                        insert into cdm.dm_settlement_report (restaurant_id, restaurant_name, settlement_date, orders_count, orders_total_sum, orders_bonus_payment_sum, orders_bonus_granted_sum, order_processing_fee, restaurant_reward_sum)
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
                        """
                    )

                    cur.execute(
                        """
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
insert into cdm.dm_courier_ledger(courier_id,courier_name,settlement_year,settlement_month,orders_count,orders_total_sum,rate_avg,order_processing_fee,courier_order_sum,courier_tips_sum,courier_reward_sum)
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
                        """
                    )