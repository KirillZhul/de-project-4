from datetime import datetime
import pendulum
from typing import List, Optional, Any

from lib.dict_util import json2str
from psycopg2.extras import execute_values
from psycopg import Connection
from lib import PgConnect
from pydantic import BaseModel
import json

import logging
import requests


log = logging.getLogger(__name__)

url = 'https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers'
headers = {
    'X-Nickname': 'zhul-kirill',
    'X-Cohort': '15',
    'X-API-KEY': '25c27781-8fde-4b30-a22e-524044a7580f'
}
params = {
    'sort_field': 'id',
    'sort_direction': 'asc',
    'limit': 50,
    'offset': 0
}

class CourierObj(BaseModel):
    # object_id: str
    object_value: str


class CourierClass:

    def __init__(self, pg: PgConnect):
         self.dwh = pg
        

    def get_data(self):
            
        params['offset'] = 0
        params['limit'] = 50

        while True:
            response = requests.get(url, headers=headers, params=params)
            data = response.json()
            result = str(data).replace("'", '"')
            if len(result) < 3:
                break
            else:
                objects = json.loads(result)
                couriers = []
                for obj in objects:
                    courier = CourierObj(object_value=str(obj))
                    log.info(str(obj))
                    couriers.append(courier)
                    
                # self.insert_courier(couriers)
                params['offset'] = params['offset'] + params['limit']

    def upload_couriers():
        conn = dwh_hook.get_conn()
        cursor = conn.cursor()
 
        # идемпотентность
        dwh_hook.run(sql=f"DELETE FROM stg.couriers")
 
        # получение данных
        offset = 0
        while True:
            couriers_rep = requests.get(f'https://{base_url}/couriers/?sort_field=_id&sort_direction=asc&offset={offset}',
                                    headers=headers).json()
 
            # останавливаемся, когда данные закончились
            if len(couriers_rep) == 0:
                conn.commit()
                cursor.close()
                conn.close()
                task_logger.info(f'Writting {offset} rows')
                break
    
            # запись в БД
            values = [[value for value in couriers_rep[i].values()] for i in range(len(couriers_rep))]
 
            sql = f"INSERT INTO stg.couriers (courier_id, courier_name) VALUES %s"
            execute_values(cursor, sql, values)
 
            offset += len(couriers_rep)