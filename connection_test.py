import requests
import json

nickname = 'zhul-kirill'
cohort = int('15') # укажите номер когорты

headers = {
    'X-Nickname': nickname,
    "X-Cohort": '15',
    "X-API-KEY": '25c27781-8fde-4b30-a22e-524044a7580f'
 }
 
API_URL_1 = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/restaurants?sort_field={{'_id','name'}}&sort_direction={{asc}}&limit={{50}}&offset={{0}}"
 
response_restaurants = requests.get(API_URL_1, headers=headers)
response_restaurants.raise_for_status()
task1 = json.loads(response_restaurants.content)


API_URL_2 = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/couriers?sort_field={{'_id','name'}}&sort_direction={{asc}}&limit={{50}}&offset={{0}}"
 
response_couriers = requests.get(API_URL_2, headers=headers)
response_couriers.raise_for_status()
task2 = json.loads(response_couriers.content)


API_URL_3 = "https://d5d04q7d963eapoepsqr.apigw.yandexcloud.net/deliveries?sort_field={{'order_id','order_ts','delivery_id','courier_id','address','delivery_ts','rate','tip_sum'}}&sort_direction={{asc}}&limit={{50}}&offset={{0}}"
 
response_deliveries = requests.get(API_URL_3, headers=headers)
response_deliveries.raise_for_status()
task3 = json.loads(response_deliveries.content)

print(task1)
print(task2)
print(task3)
