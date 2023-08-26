import logging

import pendulum
from airflow.decorators import dag, task
from examples.cdm.cdm_load import MartLoader
from lib import ConnectionBuilder

log = logging.getLogger(__name__)


@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2023, 6, 22, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'cdm', 'origin', 'example'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_project_cdm():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

    @task(task_id="load_cdm")
    def load_cdm():
        # создаем экземпляр класса, в котором реализована логика.
        cdm_loader = MartLoader(dwh_pg_connect)
        cdm_loader.load_cdm()  # Вызываем функцию, которая перельет данные.


    cdm_load = load_cdm()

    cdm_load

dds_dag = sprint5_project_cdm()