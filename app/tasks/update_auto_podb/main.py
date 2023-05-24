from sqlalchemy import create_engine
from app.tasks.config_hana import hanaLogin, hanaPassw
import datetime


def call_auto_podb():
    if datetime.datetime.today().day == 15:
        engine_HDB = create_engine('hana://{0}:{1}@10.42.40.60:30015'.format(hanaLogin, hanaPassw))
        with engine_HDB.connect() as connection_out:
            connection_out.execute("call msiukhin.update_podbor_konv();")
        with engine_HDB.connect() as connection_out:
            connection_out.execute("call msiukhin.update_podbor_cloud();")
        with engine_HDB.connect() as connection_out:
            connection_out.execute("call msiukhin.update_podbor_konv_ug();")
        with engine_HDB.connect() as connection_out:
            connection_out.execute("call msiukhin.update_podbor_konv_CRM();")


if __name__ == '__main__':
    call_auto_podb()
