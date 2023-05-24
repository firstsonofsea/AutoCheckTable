from sqlalchemy import create_engine
import traceback
from app.tasks.config_hana import hanaLogin, hanaPassw


def create_key_exeptio_main():
    # print(sql)
    engine_HDB = create_engine('hana://{0}:{1}@10.42.40.60:30015'.format(hanaLogin, hanaPassw))
    connection_out = engine_HDB.connect()
    connection_out.execute('call msiukhin.UPDATE_KEY_EXEPTION_MAIN();')
    # for i in sql:
    #     connection_out.execute(i)
        # print(i)


if __name__=='__main__':
    create_key_exeptio_main()