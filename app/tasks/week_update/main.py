from sqlalchemy import create_engine
import traceback
import datetime
from app.tasks.config_hana import hanaLogin, hanaPassw


def week_upadate():
    print(datetime.datetime.today().weekday())
    if datetime.datetime.today().weekday() == 6:
        sql = open(r'C:\Users\Mikhail.Siukhin\PycharmProjects\flask_sheduler\app\tasks\week_update\sql.txt',
                   encoding='utf-8').read().split('\n\n')
        engine_HDB = create_engine('hana://{0}:{1}@10.42.40.60:30015'.format(hanaLogin, hanaPassw))
        connection_out = engine_HDB.connect()
        for i in sql:
            connection_out.execute(i)
        # print(i)


if __name__ == '__main__':
    week_upadate()
