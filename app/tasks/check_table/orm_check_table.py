import os
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from orm_stat_data import TableInfo
import pandas as pd

basedir = os.path.abspath(os.path.dirname(__file__))
path = 'sqlite:///' + os.path.join(basedir, 'app_data.db')
engine = create_engine(path)
session = Session(bind=engine)


def load_one_item(sql, conn, session):
    table = {
        'rtk_b2c.ZB2C_CHD_TFCT_CLIENT_PROF': 'rtk_b2c.stg_ZB2C_CHD_TFCT_CLIENT_PROF',
        'rtk_b2c.ZB2C_CHD_TFCT_AOPT': 'rtk_b2c.stg_ZB2C_CHD_TFCT_AOPT',
        'rtk_b2c.ZB2C_CHD_TFCT_AMNT': 'rtk_b2c.stg_ZB2C_CHD_TFCT_AMNT',
        'rtk_b2c.ZB2C_CHD_TFCT_ALLTPB': 'rtk_b2c.stg_ZB2C_CHD_TFCT_ALLTPB',
        'rtk_b2c.ZB2C_CHD_TFCT_ASRV': 'rtk_b2c.stg_ZB2C_CHD_TFCT_ASRV',
        # 'rtk_b2c.ZB2C_CHD_TFCT_CST': 'rtk_b2c.stg_ZB2C_CHD_TFCT_CST' # нет мрф
        'rtk_b2c.ZB2C_CHD_TFCT_DH': 'rtk_b2c.stg_ZB2C_CHD_TFCT_DH'
    }

    param = {
        'rtk_b2c.ZB2C_CHD_TFCT_CLIENT_PROF': ['FIX_ON > 0',
                                              'BB_OPT_ON > 0',
                                              'BB_CPRM_ON > 0',
                                              'MVNO_ON > 0',
                                              'IPTV_ON > 0',
                                              'IPTV2_ON > 0'],
        'rtk_b2c.ZB2C_CHD_TFCT_AOPT': ['SRVS_TYPE is not null',
                                       'OPTN_NAME_FULL is not null'],
        'rtk_b2c.ZB2C_CHD_TFCT_AMNT': ['Amount_all_shpd > 0',
                                       'amount_all_iptv > 0',
                                       'amount_all_ota > 0',
                                       'amount_all_mvno > 0'],
        'rtk_b2c.ZB2C_CHD_TFCT_ALLTPB': ['SRVS_TYPE is not null',
                                         'TP_BB_SPEED > 0',
                                         'TP_FULL is not null'],
        'rtk_b2c.ZB2C_CHD_TFCT_ASRV': ['SRVS_TYPE is not null',
                                       'SRVS_TECH is not null',
                                       'TRAF_IN_MC_TOTAL > 0',
                                       'TRAF_IN_OTA > 0',
                                       'TRAF_IN_VZ > 0',
                                       'TRAF_IN_MG > 0',
                                       'TRAF_IN_MN > 0',
                                       'SRVS_MEMBER_ID is not null',
                                       ],
        'rtk_b2c.ZB2C_CHD_TFCT_CST': ['SRVS_TYPE is not null',
                                      'SRVS_TECH is not null',
                                      'FIO_CSTM is not null',
                                      'BRTHD_CSTM is not null'
                                      ],
        'rtk_b2c.ZB2C_CHD_TFCT_DH': ['HOUSE_TECH_FTTX is not null',
                                     'HOUSE_TECH_XPON is not null',
                                     'ISPRIVATE is not null',
                                     'IS_TECH_IPTV is not null',
                                     'CITY_GID is not null',
                                     'CITY_LID is not null',
                                     'CITY is not null',
                                     'NAME_STREET is not null',
                                     'FULL_ADDRESS is not null']
    }
    for i in table:
        for j in param[i]:
            for mrf in range(10, 18):
                rows = conn.execute(sql.format(i, table[i], j, mrf))
                for r in rows:
                    ins_r = TableInfo(name_t_ish=r[0],
                                      name_t=r[1],
                                      date_start=r[3],
                                      date_end=r[3],
                                      info=j,
                                      param1=mrf,
                                      info_param1="Мрф ид",
                                      param2=r[2],
                                      info_param2="Кол-во строк")
                    session.add(ins_r)


def load_dmnt(sql, conn, session):
    param_dmnt = ['SERVICE_NAME_ASR is not null',
                  'SERVICE_RTK_DETAIL_CODE is not null']
    for i in param_dmnt:
        for mrf in range(10, 18):
            rows = conn.execute(sql.format(mrf, i))
            for r in rows:
                # print(r[0], r[1], r[2], r[3])
                ins_r = TableInfo(name_t_ish=r[0],
                                  name_t=r[1],
                                  date_start=r[4],
                                  date_end=r[5],
                                  info=i,
                                  param1=mrf,
                                  info_param1="Мрф ид",
                                  param2=r[3],
                                  info_param2="Кол-во строк",
                                  param3=r[2],
                                  info_param3="Средняя сумма по charge_rub"
                                  )
                session.add(ins_r)


def load_dopt(sql, conn, session):
    param_dopt = ['SRVS_TYPE is not null',
                  'Status is not null',
                  'OPTN_NAME_FULL is not null']
    for i in param_dopt:
        for mrf in range(10, 18):
            rows = conn.execute(sql.format(mrf, i))
            for r in rows:
                # print(r[0], r[1], r[2], r[3])
                if r[3] is None or r[4] is None:
                    continue
                ins_r = TableInfo(name_t_ish=r[0],
                                  name_t=r[1],
                                  date_start=r[3],
                                  date_end=r[4],
                                  info=i,
                                  param1=mrf,
                                  info_param1="Мрф ид",
                                  param2=r[2],
                                  info_param2="Кол-во строк"
                                  )
                session.add(ins_r)


def load_blk(sql, conn, session):
    table = ['rtk_b2c.ZB2C_CHD_TFCT_BLKA', 'rtk_b2c.ZB2C_CHD_TFCT_BLKD']
    param_blk = ['BLOCK_NAME is not null',
                 'BLOCK_TYPE_NAME is not null',
                 'BLOCK_STATUS_ID is not null']
    for j in table:
        for i in param_blk:
            rows = conn.execute(sql.format(j, i))
            for r in rows:
                ins_r = TableInfo(name_t_ish=r[0],
                                  name_t=r[1],
                                  date_start=r[3],
                                  date_end=r[4],
                                  info=i,
                                  param1=r[5],
                                  info_param1="Мрф ид",
                                  param2=r[2],
                                  info_param2="Кол-во строк"
                                  )
                session.add(ins_r)


def load_itm(conn, session):
    sql = """	select 'rtk_b2c.ZB2C_SAO_WH_ITM_FULL' as name_ish_t,'rtk_b2c.ZB2C_SAO_WH_ITM_FULL' as name_t
    , count(*) as cnt, to_date(max(polldate)) as max_st, to_date(min(polldate)) as min_st
    from rtk_b2c.ZB2C_SAO_WH_ITM_FULL
    where 	
                polldate between add_days(current_date, -8) and add_days(current_date, -2)
                and lower(namebas) like '%itm%'
    
    union all
    select 'rtk_b2c.ZB2C_SAO_WH_ITM_FULL' as name_ish_t,'rtk_b2c.ZB2C_SAO_WH_ITM_FULL' as name_t
    , count(*) as cnt, to_date(max(polldate)) as max_st, to_date(min(polldate)) as min_st
    from rtk_b2c.ZB2C_SAO_WH_ITM_FULL
    where
    
        polldate between add_months(add_days(current_date, -8) ,-1) and add_months(add_days(current_date, -2), -1)
        and lower(namebas) like '%itm%'
    
    """
    rows = conn.execute(sql)
    for r in rows:
        ins_r = TableInfo(name_t_ish=r[0],
                          name_t=r[1],
                          date_start=r[3],
                          date_end=r[4],
                          info="lower(namebas) like '%itm%'",
                          param1=0,
                          info_param1="Мрф ид",
                          param2=r[2],
                          info_param2="Кол-во строк"
                          )
        session.add(ins_r)


sql1 = open('sql1.txt', encoding='utf-8').read()
sql2 = open('sql2.txt', encoding='utf-8').read()
sql3 = open('sql3.txt', encoding='utf-8').read()
sql_dmnt_1 = open('sql_dmnt.txt', encoding='utf-8').read()
sql_dmnt_2 = open('sql_dmnt1.txt', encoding='utf-8').read()
sql_dopt_1 = open('sql_dopt.txt', encoding='utf-8').read()
sql_dopt_2 = open('sql_dopt2.txt', encoding='utf-8').read()
sql_blk_1 = open('sql_blk.txt', encoding='utf-8').read()
sql_blk_2 = open('sql_blk2.txt', encoding='utf-8').read()

hanaLogin = 'MSIUKHIN'
hanaPassw = '13Yy4eBurek!'
engine_HDB = create_engine('hana://{0}:{1}@10.42.40.60:30015'.format(hanaLogin, hanaPassw))
conn = engine_HDB.connect()

load_one_item(sql1, conn, session)
load_one_item(sql2, conn, session)
load_one_item(sql3, conn, session)
load_dmnt(sql_dmnt_1, conn, session)
load_dmnt(sql_dmnt_2, conn, session)
load_dopt(sql_dopt_1, conn, session)
load_dopt(sql_dopt_2, conn, session)
load_blk(sql_blk_1, conn, session)
load_blk(sql_blk_2, conn, session)
load_itm(conn, session)

session.commit()

# for i in session.query(TableInfo).all():
#     print(i)
