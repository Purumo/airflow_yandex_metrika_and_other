## Проект airflow_yandex_metrika_and_other для выгрузки неагрегированных данных с Yandex Metrika Logs API, используя Airflow и Docker
![Картинка для описания проекта](/airflow_image.png)

Проект создан для выгрузки данных о количестве просмотров ([*hits*](https://yandex.ru/dev/metrika/doc/api2/logs/fields/hits.html)) с счётчиков Yandex Metrika во внешнее хранилище данных - БД ClickHouse. Настроена выгрузка для таких полей, как:
 - ID счётчика (`ym:pv:counterID`);
 - дата события (`ym:pv:date`);
 - соответствующий URL (`ym:pv:URL`);
 - уникальный ID пользователя (`ym:pv:counterUserIDHash`);
 - событие неотказа (`ym:pv:notBounce`);
 - *и другие*.

В целом логика работы довольна простая и основана на [официальной документации Yandex Metrika](https://yandex.ru/dev/metrika/doc/api2/logs/practice/quick-start.html):
 - для начала работы создаём *DAG*-и по количеству счётчиков, описанных в конфигурационном файле `/includes/METRIC_IDS_LIST.json`;
 - каждый *DAG* состоит из 4-х задач, результатом чего является загрузка данных в таблицу ClickHouse:
   - `create_request_to_load_data` -  реализация создания запроса лога для количестве просмотров (*hits*) `POST https://api-metrika.yandex.net/management/v1/counter/{counterId}/logrequests`. В результате получаем `request_id` и сохраняем его в *XComs* для дальнейшей работы.
   - `get_load_data_status` - ожидание статуса создания лога (`GET https://api-metrika.yandex.net/management/v1/counter/{counterId}/logrequest/{requestId}`). 
   - `extract_data` - при успешном статусе создания лога его можно выгрузить (`GET https://api-metrika.yandex.net/management/v1/counter/{counterId}/logrequest/{requestId}/part/{partNumber}/download`, в коде partNumber всегда *= 0*, т.к. предполагаем что данных немного и они влезают в одну часть выгрузки). По запросу получаем *JSON* и конвертируем его в плоскую таблицу (`data`). 
   - `load_data_to_database` - проверяем подключение к БД и загружаем `data` (~~или красим *DAG* в FAILED, если что-то пошло не так~~). 

#### Запрос для создания таблицы в Clickhouse, куда грузим данные:
``` sql
-- prod_dwh.yandex_metrika_hits definition

CREATE TABLE prod_dwh.yandex_metrika_hits
(
    `pv_watchID` UInt64,
    `time_upload` DateTime64(0),
    `pv_counterID` UInt32,
    `pv_date` Date,
    `pv_title` Nullable(String),
    `pv_URL` String,
    `pv_notBounce` Bool,
    `pv_clientID` UInt64,
    `pv_counterUserIDHash` UInt64,
    `pv_networkType` Nullable(String),
    `pv_deviceCategory` UInt8,
    `pv_artificial` Bool,
    `pv_isPageView` Bool,
    `pv_httpError` Bool,
    `pv_lastTrafficSource` String,
    `pv_from` Nullable(String),
    `pv_ipAddress` String
)
ENGINE = ReplacingMergeTree(time_upload)
PRIMARY KEY pv_watchID
ORDER BY pv_watchID
SETTINGS index_granularity = 8192;
```


### Контакты (пишите, если есть вопросы и/или предложения):
 - [Мой E-Mail](mailto:pavlovadiana@list.ru)
 - [Мой Telegram](https://t.me/Purumo)
