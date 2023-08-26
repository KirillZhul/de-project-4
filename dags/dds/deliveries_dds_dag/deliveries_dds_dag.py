import pendulum
import logging
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from examples.dds.deliveries_dds_dag.courier_loader import CourierLoader
from examples.dds.deliveries_dds_dag.deliveries_loader import DeliveryLoader

from lib import ConnectionBuilder
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.hooks.http_hook import HttpHook
from lib import ConnectionBuilder

log = logging.getLogger(__name__)

@dag(
    schedule_interval='0/15 * * * *',  # Задаем расписание выполнения дага - каждый 15 минут.
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),  # Дата начала выполнения дага. Можно поставить сегодня.
    catchup=False,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['sprint5', 'project', 'stg', 'origin'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)
def sprint5_project_dds():
    # Создаем подключение к базе dwh.
    dwh_pg_connect = ConnectionBuilder.pg_conn("PG_WAREHOUSE_CONNECTION")

   
    @task(task_id="load_couriers")
    def load_couriers():
        couriers_loader = CourierLoader(dwh_pg_connect, log)
        couriers_loader.load_couriers()  # Вызываем функцию, которая перельет данные.

    @task(task_id="load_deliveries")
    def load_deliveries():
        deliveries_loader = DeliveryLoader(dwh_pg_connect, log)
        deliveries_loader.load_deliveries()  # Вызываем функцию, которая перельет данные.

   
    # Инициализируем объявленные tasks.
    couriers_load = load_couriers()
    deliveries_load = load_deliveries()


    # Далее задаем последовательность выполнения tasks.
    couriers_load >> deliveries_load


deliveries_dds_dag = sprint5_project_dds()  