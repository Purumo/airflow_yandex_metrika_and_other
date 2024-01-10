
import requests

def get_response(request_type, url_param, api_param):
    # Отправляем get request (запрос GET)
    response = requests.request(request_type,
        url_param,
        params=api_param#,
        #headers=header_params
        )
    
    # Преобразуем response с помощью json()
    #result = response.json()
    return response