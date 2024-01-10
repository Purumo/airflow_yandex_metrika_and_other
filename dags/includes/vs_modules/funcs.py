from airflow.sensors.base_sensor_operator import BaseSensorOperator
from airflow.utils.decorators import apply_defaults
import logging

class YMDataReadySensor(BaseSensorOperator):
    @apply_defaults
    def __init__(self, metric_id, *args, **kwargs):
        super(YMDataReadySensor, self).__init__(*args, **kwargs)
        self.metric_id = metric_id

    def poke(self, context):
         # Extract request_id from XCom
        task_instance = context['ti']
        request_id = task_instance.xcom_pull(task_ids='create_request_to_load_data')

        # Check the status of the data unload request
        status = get_load_data_status(self.metric_id, request_id)
        logging.info(f'***** PRINT status FROM YMDataReadySensor: {status} *****')
    
        if status == 'processed':
            return True
        return False
    
def get_response(request_type, url_param, api_param):
    import requests
    import os

    access_token = os.getenv('ACCESS_TOKEN')   
    
    # Задаем параметры header_params
    header_params = {
        'GET': '/management/v1/counters HTTP/1.1',
        'Host': 'api-metrika.yandex.net',
        'Authorization': 'OAuth ' + access_token,
        'Content-Type': 'application/x-yametrika+json'
        }

    ###########PROD###########
    response = requests.request(
        request_type, 
        url_param, 
        params=api_param, 
        headers=header_params
    )
    ##########################
    
    #response = 'processed' #FOR TESTS

    return response

def create_request_to_load_data(metric_id, **context):
    import json
    
    logging.info(f'***** PRINT context FROM create_request_to_load_data function: {context} *****')
    
    # Вычисляем дату начала и дату окончания
    DayStart = context['ds']
    DayEnd = DayStart
    
    request_type = "POST"
    url_param = f'https://api-metrika.yandex.net/management/v1/counter/{metric_id}/logrequests'

    # Задаем параметры для API
    api_param = {
                "date1":DayStart,
                "date2":DayEnd, 
                "fields":"ym:pv:watchID,ym:pv:counterID,ym:pv:date,ym:pv:title,ym:pv:URL,"
                    + "ym:pv:notBounce,ym:pv:clientID,ym:pv:counterUserIDHash,ym:pv:networkType,"
                    + "ym:pv:deviceCategory,ym:pv:artificial,ym:pv:isPageView,ym:pv:httpError,"
                    + "ym:pv:lastTrafficSource,ym:pv:from,ym:pv:ipAddress",
                "source":"hits",
                "auto_cleaning":"true"
                }
    logging.info('request: ', request_type, url_param, api_param)
    
    ###########PROD###########
    result = json.dumps(get_response(request_type, url_param, api_param).json())
    request_id = json.loads(result)['log_request']['request_id']
    ##########################
    
    #request_id = 666 #FOR TESTS
    
    logging.info(f'Создан запрос на вызгрузку данных с request_id = "{request_id}"')
    
    return request_id    

def get_load_data_status(metric_id, request_id):
    import json
    import time
    logging.info('***** PRINT FROM get_load_data_status function *****')

    request_type = "GET"
    url_param = f'https://api-metrika.yandex.net/management/v1/counter/{metric_id}/logrequest/{request_id}/'
    api_param = []
    
    logging.info(request_type, url_param)
    
    ###########PROD###########
    result = json.dumps(get_response(request_type, url_param, api_param).json())
    request_status = json.loads(result)['log_request']['status']
    ##########################
    
    #request_status = 'processed' #FOR TESTS
    return request_status


def extract_data(metric_id, **context):
    import io
    import pandas as pd
    from datetime import datetime, timedelta

    print('***** PRINT FROM extract_data function *****')

    # Extract request_id from XCom
    task_instance = context['ti']
    request_id = task_instance.xcom_pull(task_ids='create_request_to_load_data')

    request_type = "GET"
    url_param = f'https://api-metrika.yandex.net/management/v1/counter/{metric_id}/logrequest/{request_id}/part/0/download/'
    api_param = []

    logging.info('request: ', request_type, url_param, api_param)

    logging.info(f"Type of metric_id ({metric_id}): {type(metric_id)}")
    logging.info(f"Type of request_id ({request_id}): {type(request_id)}")

    response = get_response(request_type, url_param, api_param) #PROD
    #response = 'some response' #FOR TESTS

    logging.info(f'Получен ответ по запросу загрузки данных: {response}')

    ###########PROD###########
    response_data = response.text
            
    # Используем StringIO для преобразования строки в файлоподобный объект
    data_io = io.StringIO(response_data)
            
    # Чтение данных в DataFrame, разделитель - пробел
    out_data = pd.read_csv(data_io, sep='\t')

    logging.info(f'Получены данные с кол-вом строк {out_data.shape[0]}') 
    ##########################
    
    columns = [
        'pv_watchID',
        'time_upload',
        'pv_counterID',
        'pv_date',
        'pv_title',
        'pv_URL',
        'pv_notBounce',
        'pv_clientID',
        'pv_counterUserIDHash',
        'pv_networkType',
        'pv_deviceCategory',
        'pv_artificial',
        'pv_isPageView',
        'pv_httpError',
        'pv_lastTrafficSource',
        'pv_from',
        'pv_ipAddress'
        ]

    ###########PROD###########
    data = pd.DataFrame(columns = columns)

    data['pv_watchID'] = out_data['ym:pv:watchID'].astype('UInt64')
    data['time_upload'] = datetime.now()  #.strftime("%Y-%m-%d %H:%M:%S")
    data['pv_counterID'] = out_data['ym:pv:counterID'].astype('UInt32')
    data['pv_date'] = pd.to_datetime(out_data['ym:pv:date'], format = "%Y-%m-%d")
    data['pv_title'] = out_data['ym:pv:title'].astype('string')
    data['pv_URL'] = out_data['ym:pv:URL'].astype('string')
    data['pv_notBounce'] = out_data['ym:pv:notBounce'].astype(bool)
    data['pv_clientID'] = out_data['ym:pv:clientID'].astype('UInt64')
    data['pv_counterUserIDHash'] = out_data['ym:pv:counterUserIDHash'].astype('UInt64')
    data['pv_networkType'] = out_data['ym:pv:networkType'].astype('string')
    data['pv_deviceCategory'] = out_data['ym:pv:deviceCategory'].astype('UInt8')
    data['pv_artificial'] = out_data['ym:pv:artificial'].astype(bool)
    data['pv_isPageView'] = out_data['ym:pv:isPageView'].astype(bool)
    data['pv_httpError'] = out_data['ym:pv:httpError'].astype(bool)
    data['pv_lastTrafficSource'] = out_data['ym:pv:lastTrafficSource'].astype('string')
    data['pv_from'] = out_data['ym:pv:from'].astype('string')
    data['pv_ipAddress'] = out_data['ym:pv:ipAddress'].astype('string')
    ##########################

    ########FOR TESTS#########
    '''
    row_data = [
            666,
            datetime.now(),
            444,
            pd.to_datetime(datetime.now(), format = "%Y-%m-%d"),
            str(metric_id) + ' ALALA: ' + str(request_id),
            'ALLALAL',
            True,
            88,
            8,
            'ALLALAL',
            8,
            False,
            True,
            False,
            'ALLALAL',
            'ALLALAL',
            'ALLALAL'
    ]
    data = pd.DataFrame([row_data], columns=columns)
    '''
    ##########################

    return data


def load_data_to_database(**context):
    import os
    import clickhouse_connect
    
    logging.info('***** PRINT FROM load_data_to_database function *****')
    
    # Extract request_id from XCom
    task_instance = context['ti']
    data = task_instance.xcom_pull(task_ids='extract_data')

    logging.info('Extract request_id from XCom: ', data)

    ch_host = os.getenv('CH_HOST')
    ch_port = os.getenv('CH_PORT')
    ch_username = os.getenv('CH_USERNAME')
    ch_password = os.getenv('CH_PASSWORD')

    try:
        client = clickhouse_connect.get_client(
            host=ch_host, port=ch_port, username=ch_username, password=ch_password)
        
        logging.info(f'Подключение к серверу прошло успешно! {client}')

    except Exception as e:
        logging.info(f'Возникла ошибка при подключению к серверу! \n{e}')
        raise Exception("An error occurred in task clickhouse_connect.get_client")
    
    try:
        result = client.insert('prod_dwh.yandex_metrika_hits', data, 
                                   column_names = [
                                        'pv_watchID',
                                        'time_upload',
                                        'pv_counterID',
                                        'pv_date',
                                        'pv_title',
                                        'pv_URL',
                                        'pv_notBounce',
                                        'pv_clientID',
                                        'pv_counterUserIDHash',
                                        'pv_networkType',
                                        'pv_deviceCategory',
                                        'pv_artificial',
                                        'pv_isPageView',
                                        'pv_httpError',
                                        'pv_lastTrafficSource',
                                        'pv_from',
                                        'pv_ipAddress'
                                   ] 
                                )
        
        logging.info(f'Данные должны были загрузиться! {result}')

    except Exception as e:
        logging.info(f'Возникла ошибка при вставке данных! \n{e}')
        raise Exception("An error occurred in task client.insert")
        
            
    


















