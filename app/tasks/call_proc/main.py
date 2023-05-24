from sqlalchemy import create_engine
from app.tasks.config_hana import hanaLogin, hanaPassw


def call_proc():
    # print(sql)
    engine_HDB = create_engine('hana://{0}:{1}@10.42.40.60:30015'.format(hanaLogin, hanaPassw))
    connection_out = engine_HDB.connect()
    connection_out.execute("call msiukhin.P_COLLECTING_EXCEPTIONS();")


if __name__ == '__main__':
    call_proc()
