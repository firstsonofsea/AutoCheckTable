from sqlalchemy import create_engine
import traceback
import datetime

from app.tasks.config_hana import hanaLogin, hanaPassw


def load_comp_key():
    if datetime.datetime.today().day == 11 or datetime.datetime.today().day == 26:
        sql = open(r'C:\Users\Mikhail.Siukhin\PycharmProjects\flask_sheduler\app\tasks\load_comp_key\sql.txt',
                   encoding='utf-8').read().split('\n\n')
        # print(sql)
        engine_HDB = create_engine('hana://{0}:{1}@10.42.40.60:30015'.format(hanaLogin, hanaPassw))
        connection_out = engine_HDB.connect()
        for i in sql:
            connection_out.execute(i)
            # print(i)