from sqlalchemy import create_engine
import pandas as pd
from app.tasks.config_hana import hanaLogin, hanaPassw


def check_table():
    path = r'C:\Users\Mikhail.Siukhin\PycharmProjects\flask_sheduler\app\tasks\check_table'
    engine_HDB = create_engine('hana://{0}:{1}@10.42.40.60:30015'.format(hanaLogin, hanaPassw))
    conn = engine_HDB.connect()

    all_table = [
        'rtk_b2c.ZB2C_CHD_TFCT_CLIENT_PROF',
        'rtk_b2c_business.CHD_TFCT_CLIENT_AGG',
        'rtk_b2c.ZB2C_CHD_TFCT_AMNT',
        'rtk_b2c.ZB2C_CHD_TFCT_DMNT',
        'rtk_b2c.ZB2C_CHD_TFCT_AOPT',
        'rtk_b2c.ZB2C_CHD_TFCT_DOPT',
        'rtk_b2c.ZB2C_CHD_TFCT_ASRV',
        'rtk_b2c.ZB2C_CHD_TFCT_ALLTPB',
        'rtk_b2c.ZB2C_CHD_TFCT_CST',
        'rtk_b2c.ZB2C_CHD_TFCT_BLKA',
        'rtk_b2c.ZB2C_CHD_TFCT_BLKD',
        'rtk_b2c.ZB2C_CHD_TFCT_DH'
    ]

    sql = "select '{}' as name,current_date, count(*) as count_str from {}"
    sql_result = ''
    for i in all_table[:-1]:
        sql_result += sql.format(i, i) + '\nunion all\n'
    sql_result += sql.format(all_table[-1], all_table[-1])

    df = pd.read_sql(sql_result, conn)
    df1 = pd.read_csv(path + r'\old_data.csv')
    df1 = pd.merge(left=df1, right=df, on='name')
    df1['delta'] = round((df1['count_str_x'] / df1['count_str_y'] - 1) * 100, 2)
    df1.to_csv(path + r'\result.csv', index=False)
    df.to_csv(path + r'\old_data.csv', index=False)
