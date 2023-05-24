from sqlalchemy import create_engine
import pandas as pd
from app.tasks.config_hana import hanaLogin, hanaPassw
import win32com


def check_max_eff_dttm():
    engine_HDB = create_engine('hana://{0}:{1}@10.42.40.60:30015'.format(hanaLogin, hanaPassw))
    conn = engine_HDB.connect()

    sql = "select mrf_id, max(eff_dttm) from {0} group by mrf_id"

    table_name = ["rtk_b2c.ZB2C_CHD_TFCT_cntr",
                  "rtk_b2c.ZB2C_CHD_TFCT_client_prof",
                  "rtk_b2c.ZB2C_CHD_TFCT_amnt",
                  "rtk_b2c.ZB2C_CHD_TFCT_dmnt",
                  "rtk_b2c.ZB2C_CHD_TFCT_asrv",
                  "rtk_b2c.ZB2C_CHD_TFCT_alltpb"]

    table_name_opt = ["rtk_b2c.ZB2C_CHD_TFCT_AOPT",
                      "rtk_b2c.ZB2C_CHD_TFCT_DOPT"]
    sql_opt_1 = "select mrf_id, max(eff_dttm) from {0} where src_id <> 45 and mrf_id <> 15 group by mrf_id"
    sql_opt_2 = "select mrf_id, max(eff_dttm) from {0} where mrf_id = 15 group by mrf_id"

    s = '<html><table border="2">\n'
    for table in table_name:
        df = pd.read_sql(sql.format(table), conn)
        for _, j in df.iterrows():
            s += f"<tr><td>{table}</td><td>{int(j[0])}</td><td>{j[1]}</td></tr>\n"

    for table in table_name_opt:
        df = pd.read_sql(sql_opt_1.format(table), conn)
        for _, j in df.iterrows():
            s += f"<tr><td>{table}</td><td>{int(j[0])}</td><td>{j[1]}</td></tr>\n"
        df = pd.read_sql(sql_opt_2.format(table), conn)
        for _, j in df.iterrows():
            s += f"<tr><td>{table}</td><td>{int(j[0])}</td><td>{j[1]}</td></tr>\n"

    sql2 = 'select mrf_id, max(prd_dttm ) from rtk_B2c_business.TFCT_ADVG_CNNT_STOP_LIST group by mrf_id'
    df = pd.read_sql(sql2, conn)
    for _, j in df.iterrows():
        s += f"<tr><td>{'rtk_B2c_business.TFCT_ADVG_CNNT_STOP_LIST'}</td><td>{int(j[0])}</td><td>{j[1]}</td></tr>\n"

    sql3 = 'select mrf_id, max(snapshot_dt) from rtk_b2c.ZB2C_CHD_TFCT_BLKA group by mrf_id'
    df = pd.read_sql(sql3, conn)
    for _, j in df.iterrows():
        s += f"<tr><td>{'rtk_b2c.ZB2C_CHD_TFCT_BLKA'}</td><td>{int(j[0])}</td><td>{j[1]}</td></tr>\n"

    s += '</talbe></html>'

    o = win32com.client.Dispatch("Outlook.Application")

    Msg = o.CreateItem(0)
    Msg.To = """
        mikhail.siukhin@south.rt.ru;Nikita.Sitolik@rt.ru"""
    # Msg.To = "mikhail.siukhin@south.rt.ru"
    Msg.Subject = "Проверка даты последнего среза"
    Msg.HTMLBody = s
    Msg.Send()


if __name__ == '__main__':
    check_max_eff_dttm()
