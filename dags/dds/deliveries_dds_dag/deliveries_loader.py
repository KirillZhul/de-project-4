import json
import logging
from datetime import datetime
from lib import PgConnect
from logging import Logger
from typing import List, Optional
from examples.dds.dds_settings_repository import DdsEtlSettingsRepository, EtlSetting
from examples.dds.deliveries_dds_dag.courier_loader import CourierDdsObj, CourierDestRepository
from psycopg import Connection
from lib.dict_util import json2str
from psycopg.rows import class_row
from pydantic import BaseModel


log = logging.getLogger(__name__)

class DeliveryJsonObj(BaseModel):
    id: int
    object_value: str
    update_ts: datetime

class DeliveryDdsObj(BaseModel):
    id: int
    delivery_id: str
    courier_id: str
    order_id: str
    order_ts: datetime
    rate: float
    tip_sum: float
    sum: float
    delivery_ts: datetime

	
class DeliveryRawRepository:
    def load_raw_deliveries(self, conn: Connection, last_loaded_record_id: int) -> List[DeliveryJsonObj]:
        with conn.cursor(row_factory=class_row(DeliveryJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        replace(object_value, '''', '"') as object_value,
                        update_ts
                    FROM stg.delivery
                    WHERE id > %(last_loaded_record_id)s
                    ORDER BY id ASC;
                """,
                {"last_loaded_record_id": last_loaded_record_id},
            )
            objs = cur.fetchall()
        objs.sort(key=lambda x: x.id)
        return objs


class DeliveryDdsRepository:
    def insert_delivery(self, conn: Connection, delivery: DeliveryDdsObj) -> None:
        with conn.cursor() as cur:
            cur.execute(
                """
                    INSERT INTO dds.fct_deliveries(delivery_id, order_id, order_ts, courier_id, rate, tip_sum, sum, delivery_ts)
                    VALUES (%(delivery_id)s, %(order_id)s, %(order_ts)s, %(courier_id)s, %(rate)s, %(tip_sum)s, %(sum)s, %(delivery_ts)s)
                    ON CONFLICT (delivery_id) DO UPDATE
                    SET
                        order_id = EXCLUDED.order_id,
                        courier_id = EXCLUDED.courier_id,
                        order_ts = EXCLUDED.order_ts,
                        rate = EXCLUDED.rate,
                        tip_sum = EXCLUDED.tip_sum,
                        sum = EXCLUDED.sum,
                        delivery_ts = EXCLUDED.delivery_ts
                    ;
                """,
                {
                    "delivery_id": delivery.delivery_id,
                    "order_id": delivery.order_id,
                    "order_ts": delivery.order_ts,
                    "courier_id": delivery.courier_id,
                    "rate": delivery.rate,
                    "tip_sum": delivery.tip_sum,
                    "sum": delivery.sum,
                    "delivery_ts": delivery.delivery_ts
                },
            )

    def get_delivery(self, conn: Connection, delivery_id: str) -> Optional[DeliveryDdsObj]:
        with conn.cursor(row_factory=class_row(DeliveryDdsObj)) as cur:
            cur.execute(
                """
                    SELECT
                        id,
                        delivery_id,
                        courier_id,
                        order_id,
                        order_ts,
                        rate,
                        tip_sum,
                        sum,
                        delivery_ts
                    FROM dds.fct_deliveries
                    WHERE delivery_id = %(delivery_id)s;
                """,
                {"delivery_id": delivery_id},
            )
            obj = cur.fetchone()
        return obj

class DeliveryLoader:
    WF_KEY = "deliveries_raw_to_dds_workflow"
    LAST_LOADED_ID_KEY = "last_loaded_id"

    def __init__(self, pg: PgConnect, log: Logger) -> None:
        self.dwh = pg
        self.raw = DeliveryRawRepository()
        self.dds_couriers = CourierDestRepository()
        self.dds_deliveries = DeliveryDdsRepository()
        self.settings_repository = DdsEtlSettingsRepository()
        self.log = log

    def parse_delivery(self, delivery_raw: DeliveryJsonObj, courier_id: int) -> DeliveryDdsObj:
        delivery_json = json.loads(delivery_raw.object_value)

        t = DeliveryDdsObj(id=0,
                        delivery_id=delivery_json['delivery_id'],
                        courier_id=delivery_json['courier_id'],
                        order_id=delivery_json['order_id'],
                        order_ts=delivery_json['order_ts'],
                        rate=delivery_json['rate'],
                        tip_sum=delivery_json['tip_sum'],
                        sum=delivery_json['sum'],
                        delivery_ts=delivery_json['delivery_ts']
                        )

        return t
    
    def load_deliveries(self):
        with self.dwh.connection() as conn:
            wf_setting = self.settings_repository.get_setting(conn, self.WF_KEY)
            if not wf_setting:
                wf_setting = EtlSetting(id=0, workflow_key=self.WF_KEY, workflow_settings={self.LAST_LOADED_ID_KEY: -1})

            last_loaded_id = wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY]

            load_queue = self.raw.load_raw_deliveries(conn, last_loaded_id)
            load_queue.sort(key=lambda x: x.id)
            self.log.info('LOG 1:  ' + str(load_queue))
            for delivery_raw in load_queue:

                delivery_json = json.loads(delivery_raw.object_value)
            
                courier = self.dds_couriers.get_courier(conn, delivery_json['courier_id'])
                if not courier:
                    break
              
                delivery_to_load = self.parse_delivery(delivery_raw, courier[0])
                self.dds_deliveries.insert_delivery(conn, delivery_to_load)

                wf_setting.workflow_settings[self.LAST_LOADED_ID_KEY] = delivery_raw.id
                wf_setting_json = json2str(wf_setting.workflow_settings)
                self.settings_repository.save_setting(conn, wf_setting.workflow_key, wf_setting_json)

                self.log.info(f"Finishing work. Last checkpoint: {wf_setting_json}")