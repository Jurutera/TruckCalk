import os
import ydb

# Этот скрипт обращается к табличке стоимостей фрахта и возвращает значение
# В переменных окружения задается подключение к табличке, откуда забрать, куда доставить
# и на какую едницу техники необходимо получить стоимость

# Скрипт работает так: Из таблички получается строка с парой ID_START_POINT ID_END_POINT
# Дальше из строки возвращается требуемое значение по стоимости для ID_CARRIER

# Создадим driver для подключения к БД.
# YDB_ENDPOINT и YDB_DATABASE задаются на уровне функции

driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)
# Заведем скрипт:
qwery = """
    DECLARE $id_start_point AS int64;
    DECLARE $id_end_point AS int64;

    SELECT 
        *
    FROM 
        carrieRCosTTable
    WHERE  
        ID_START_POINT = $id_start_point and ID_END_POINT = $id_end_point
    """


def handler(event, context):
    id_carrier = event['queryStringParameters'].get('id_carrier')
    id_end_point = event['queryStringParameters'].get('id_end_point')
    id_start_point = event['queryStringParameters'].get('id_start_point')

    def execute_query(session):
        prepared_query = session.prepare(qwery)
        # create the transaction and execute query / Начинаем транзакцию и создаем запрос.
        return session.transaction().execute(prepared_query,
                                             {
                                                 '$ID_START_POINT': id_start_point,
                                                 '$ID_END_POINT': id_end_point,
                                             },
                                             commit_tx=True,
                                             settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(
                                                 2)
                                             )

    # Execute query with the retry_operation helper.
    result = pool.retry_operation_sync(execute_query)
    return {
        'statusCode': 200,
        'body': int(result[0].rows[0].get(id_carrier)),
    }
