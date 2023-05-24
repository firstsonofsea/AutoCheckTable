from sqlalchemy import create_engine
import traceback
from app.tasks.config_hana import hanaLogin, hanaPassw


def sale_available():
    sql = open(r'C:\Users\Mikhail.Siukhin\PycharmProjects\flask_sheduler\app\tasks\sale_available\sql.txt',
               encoding='utf-8').read().split('\n\n')
    engine_HDB = create_engine('hana://{0}:{1}@10.42.40.60:30015'.format(hanaLogin, hanaPassw))
    connection_out = engine_HDB.connect()
    for i in sql:
        connection_out.execute(i)
        # print(i)
