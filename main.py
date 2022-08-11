import os
import ydb

# Этот скрипт обращается к табличке стоимостей фрахта и возвращает значение
# В переменных окружения задается подключение к табличке, откуда забрать, куда доставить
# и на какую едницу техники необходимо получить стоимость

# Скрипт работает так: Из таблички получается строка с парой ID_START_POINT ID_END_POINT
# Дальше из строки возвращается требуемое значение по стоимости для ID_CARRIER


# create driver in global space.
driver = ydb.Driver(endpoint=os.getenv('YDB_ENDPOINT'), database=os.getenv('YDB_DATABASE'))
# Wait for the driver to become active for requests.
driver.wait(fail_fast=True, timeout=5)
# Create the session pool instance to manage YDB sessions.
pool = ydb.SessionPool(driver)


def handler(event, context):
    # Получаем параметры запроса из event
    id_carrier = event['queryStringParameters'].get('id_carrier')
    id_end_point = int(event['queryStringParameters'].get('id_end_point'))
    id_start_point = int(event['queryStringParameters'].get('id_start_point'))

    def execute_query(session):
        # create the transaction and execute query / Начинаем транзакцию и создаем запрос.
        query = """
            DECLARE $id_start_point AS Int;
            DECLARE $id_end_point AS Int;
            SELECT 
                *
            FROM 
                `carrieRCosTTable`
            WHERE  
                ID_START_POINT = $id_start_point and ID_END_POINT = $id_end_point
            """
        prepared_query = session.prepare(query)
        return session.transaction().execute(prepared_query,
                                            {
                                              '$id_start_point':  id_start_point,
                                              '$id_end_point' : id_end_point,
                                            },
                                             commit_tx=True,
                                             settings=ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(
                                                 2)
                                             )

    # Execute query with the retry_operation helper.
    result = pool.retry_operation_sync(execute_query)
    #Собираем логи транзакции:
    print(f"id_carrier = {id_carrier}. id_start_point = {id_start_point}, {type(id_start_point)}.id_end_point = {id_end_point}, {type(id_end_point)}.result_set = {result[0].rows[0]} result_func = {result[0].rows[0].get(id_carrier)}")
    return {
        'statusCode': 200,
        'body': result[0].rows[0].get(id_carrier),  # ответ в Int
    }
