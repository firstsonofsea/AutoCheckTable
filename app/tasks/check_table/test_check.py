import os
import traceback

from sqlalchemy import create_engine
import pandas as pd
import win32com.client
import datetime
from app.tasks.config_hana import hanaLogin, hanaPassw

def update_files(conn):
    filepath = r'C:\Users\Mikhail.Siukhin\PycharmProjects\flask_sheduler\app\tasks\check_table'
    sql_shab_dmnt = open(filepath + r'\shab1.txt', encoding='utf-8').read()

    sql_shab_dopt = open(filepath + r'\shab_dopt.txt', encoding='utf-8').read()

    sql_shab_blk = open(filepath + r'\shab_blk.txt',  encoding='utf-8').read()

    sql_shab_itm = open(filepath + r'\shab_itm.txt',  encoding='utf-8').read()

    sql_shab_amnt = open(filepath + r'\shab_amnt.txt',  encoding='utf-8').read()

    sql_shab_cst = open(filepath + r'\shab_cst.txt',  encoding='utf-8').read()

    sql_shab_one_srav = open(filepath + r'\shab_one.txt', encoding='utf-8').read()

    sql_shab_aopt = open(filepath + r'\shab_aopt.txt', encoding='utf-8').read()

    sql_shab_blka = open(filepath + r'\shab_blka.txt',  encoding='utf-8').read()

    table_one = {
                 'rtk_b2c.ZB2C_CHD_TFCT_CLIENT_PROF': 'rtk_b2c.stg_ZB2C_CHD_TFCT_CLIENT_PROF',
                 # 'rtk_b2c.ZB2C_CHD_TFCT_AOPT': 'rtk_b2c.stg_ZB2C_CHD_TFCT_AOPT',
                 'rtk_b2c.ZB2C_CHD_TFCT_ALLTPB': 'rtk_b2c.stg_ZB2C_CHD_TFCT_ALLTPB',
                 'rtk_b2c.ZB2C_CHD_TFCT_ASRV': 'rtk_b2c.stg_ZB2C_CHD_TFCT_ASRV',
                 'rtk_b2c.ZB2C_CHD_TFCT_DH': 'rtk_b2c.stg_ZB2C_CHD_TFCT_DH',
                 'rtk_b2c.ZB2C_CHD_TFCT_CNTR': 'rtk_b2c.stg_ZB2C_CHD_TFCT_CNTR',
                 'rtk_b2c.ZB2C_CHD_TFCT_KSCM': 'rtk_b2c.stg_ZB2C_CHD_TFCT_KSCM'
                 }

    name_file = {'rtk_b2c.ZB2C_CHD_TFCT_CLIENT_PROF': 'CLIENT_PROF',
                 # 'rtk_b2c.ZB2C_CHD_TFCT_AOPT': 'AOPT',
                 'rtk_b2c.ZB2C_CHD_TFCT_ALLTPB': 'ALLTPB',
                 'rtk_b2c.ZB2C_CHD_TFCT_ASRV': 'ASRV',
                 'rtk_b2c.ZB2C_CHD_TFCT_DH': 'DH',
                 'rtk_b2c.ZB2C_CHD_TFCT_CNTR': 'CNTR',
                 'rtk_b2c.ZB2C_CHD_TFCT_KSCM': 'KSCM'}

    param_dmnt = ["SERVICE_NAME_ASR is not null and service_name_asr <> '' and service_name_asr <> 'Не определено' and SERVICE_NAME_ASR <> 'R'",
                  "SERVICE_RTK_DETAIL_CODE is not null and SERVICE_RTK_DETAIL_CODE<> 'R' and SERVICE_RTK_DETAIL_CODE <> 'Не определено'"]

    param_dopt = ["OPTN_NAME_FULL is not null and OPTN_NAME_FULL <> '' and OPTN_NAME_FULL <> 'Не определено'"]

    param_blk = ["BLOCK_STATUS_ID is not null and block_status_id <> 2"]

    param_cst = ["FIO_CSTM is not null and FIO_CSTM<>'' and FIO_CSTM<>'Не определено'",
                 "BRTHD_CSTM is not null"]

    param_aopt = ["OPTN_NAME_FULL is not null and OPTN_NAME_FULL <> '' and OPTN_NAME_FULL <> 'Не определено'"]

    param_text = {'rtk_b2c.ZB2C_CHD_TFCT_CLIENT_PROF': {'FIX_ON > 0': 'Отклонение кол-ва клиентов с услугой ОТА превысило пороговое значение в МРФ ',
                                                        'BB_OPT_ON > 0': 'Отклонение кол-ва клиентов с услугой ШПД Оптика превысило пороговое значение в МРФ ',
                                                        'BB_CPRM_ON > 0': 'Отклонение кол-ва клиентов с услугой ШПД Медь превысило пороговое значение в МРФ ',
                                                        'MVNO_ON > 0': 'Отклонение кол-ва клиентов с услугой MVNO превысило пороговое значение в МРФ ',
                                                        'IPTV_ON > 0': 'Отклонение кол-ва клиентов с услугой IPTV превысило пороговое значение в МРФ ',
                                                        'IPTV2_ON > 0': 'Отклонение кол-ва клиентов с услугой IPTV2 превысило пороговое значение в МРФ '
                                                        },
                  'rtk_b2c.ZB2C_CHD_TFCT_AOPT': {"SRVS_TYPE is not null and SRVS_TYPE <> ''": 'Отклонение кол-ва записей с заполненным типом услуги превысило пороговое значение в МРФ ',
                                                 "OPTN_NAME_FULL is not null and OPTN_NAME_FULL <> '' and OPTN_NAME_FULL <> 'Не определено'": 'Отклонение кол-ва записей с заполненным названием услуги превысило пороговое значение в МРФ '
                                                 },
                  'rtk_b2c.ZB2C_CHD_TFCT_AMNT': {'Amount_all_shpd > 0':
                                                     ['Отклонение кол-ва клиентов с начислениями за ШПД превысило пороговое значение в МРФ ',
                                                      'Отклонение среднего начисления за ШПД превысило пороговое значение в МРФ '],
                                                 'amount_all_iptv > 0':
                                                     ['Отклонение кол-ва клиентов с начислениями за IPTV превысило пороговое значение в МРФ ',
                                                      'Отклонение среднего начисления за IPTV превысило пороговое значение в МРФ '],
                                                 'amount_all_ota > 0':
                                                     ['Отклонение кол-ва клиентов с начислениями за OTA превысило пороговое значение в МРФ ',
                                                      'Отклонение среднего начисления за OTA превысило пороговое значение в МРФ '],
                                                 'amount_all_mvno > 0':
                                                     ['Отклонение кол-ва клиентов с начислениями за MVNO превысило пороговое значение в МРФ ',
                                                      'Отклонение среднего начисления за MVNO превысило пороговое значение в МРФ '],
                                                 },
                  'rtk_b2c.ZB2C_CHD_TFCT_ALLTPB': {"SRVS_TYPE is not null and SRVS_TYPE <> ''": 'Отклонение кол-ва записей с заполненным полем Тип услуги превысило пороговое значение МРФ ',
                                                   'TP_BB_SPEED > 0': 'Отклонение кол-ва записей с заполненным полем Скорость тарифа ШПД превысило пороговое значение в МРФ ',
                                                   "TP_FULL is not null and TP_FULL <> ''": 'Отклонение кол-ва записей с заполненным полем Наименование тарифа ШПД превысило пороговое значение в МРФ '
                                                   },
                  'rtk_b2c.ZB2C_CHD_TFCT_ASRV': {"SRVS_TYPE is not null and SRVS_TYPE <> ''": 'Отклонение кол-ва записей с заполненным полем Тип услуги превысило пороговое значение в МРФ ',
                                                 "SRVS_TECH is not null and SRVS_TECH <> ''": 'Отклонение кол-ва записей с заполненным полем Технология услуги превысило пороговое значение в МРФ ',
                                                 'TRAF_IN_MC_TOTAL > 0': 'Отклонение кол-ва записей с заполненным полем Трафик местной связи ОТА превысило пороговое значение в МРФ ',
                                                 'TRAF_IN_OTA > 0': 'Отклонение кол-ва записей с заполненным полем Общий трафик ОТА превысило пороговое значение в МРФ ',
                                                 'TRAF_IN_VZ > 0': 'Отклонение кол-ва записей с заполненным полем Внутризоновый трафик ОТА превысило пороговое значение в МРФ ',
                                                 'TRAF_IN_MG > 0': 'Отклонение кол-ва записей с заполненным полем Междугородний трафик ОТА превысило пороговое значение в МРФ ',
                                                 'TRAF_IN_MN > 0': 'Отклонение кол-ва записей с заполненным полем Международный трафик ОТА превысило пороговое значение в МРФ ',
                                                 "SRVS_MEMBER_ID is not null and SRVS_MEMBER_ID <> ''": 'Отклонение кол-ва записей с заполненным полем Логин услуги превысило пороговое значение в МРФ '
                                                 },
                  'rtk_b2c.ZB2C_CHD_TFCT_DH': {'HOUSE_TECH_FTTX is not null': 'Отклонение кол-ва записей с заполненным полем Доступность технологии FTTX превысило пороговое значение в МРФ ',
                                       'HOUSE_TECH_XPON is not null': 'Отклонение кол-ва записей с заполненным полем Доступность технологии xPON превысило пороговое значение в МРФ ',
                                       "CITY_GID is not null and CITY_GID <> ''": 'Отклонение кол-ва записей с заполненным полем ID города GID превысило пороговое значение в МРФ ',
                                       "CITY_LID is not null and CITY_LID <> ''": 'Отклонение кол-ва записей с заполненным полем ID города LID превысило пороговое значение в МРФ ',
                                       "CITY is not null and CITY <> ''": 'Отклонение кол-ва записей с заполненным полем Наименование населенного пункта превысило пороговое значение в МРФ ',
                                       "FULL_ADDRESS is not null and FULL_ADDRESS <> ''": 'Отклонение кол-ва записей с заполненным полем Полный адрес клиента превысило пороговое значение в МРФ '
                                       },
                  'dmnt': {"SERVICE_NAME_ASR is not null and service_name_asr <> '' and service_name_asr <> 'Не определено' and SERVICE_NAME_ASR <> 'R'":
                           'Отклонение кол-ва записей с заполненным SERVICE_NAME_ASR превысило пороговое значение в МРФ ',
                           "SERVICE_RTK_DETAIL_CODE is not null and SERVICE_RTK_DETAIL_CODE<> 'R' and SERVICE_RTK_DETAIL_CODE <> 'Не определено'":
                           'Отклонение кол-ва записей с заполненным SERVICE_RTK_DETAIL_CODE превысило пороговое значение в МРФ ',
                           "SERVICE_NAME_ASR is not null and service_name_asr <> '' and service_name_asr <> 'Не определено' and SERVICE_NAME_ASR <> 'R'_sum":
                           'Отклонение cуммы у пользователей с заполненным SERVICE_NAME_ASR превысило пороговое значение в МРФ ',
                           "SERVICE_RTK_DETAIL_CODE is not null and SERVICE_RTK_DETAIL_CODE<> 'R' and SERVICE_RTK_DETAIL_CODE <> 'Не определено'_sum":
                           'Отклонение cуммы у пользователей с заполненным SERVICE_RTK_DETAIL_CODE превысило пороговое значение в МРФ '
                           },
                  'dopt': {"SRVS_TYPE is not null and SRVS_TYPE <> ''": 'Отклонение кол-ва записей с заполненным Типом услуги превысило пороговое значение в МРФ ',
                           "Status is not null and Status <> ''": 'Отклонение кол-ва записей с заполненным Статусом подключения превысило пороговое значение в МРФ ',
                           "OPTN_NAME_FULL is not null and OPTN_NAME_FULL <> '' and OPTN_NAME_FULL <> 'Не определено'": 'Отклонение кол-ва записей с заполненным Названием услуги превысило пороговое значение в МРФ '
                           },
                  'rtk_b2c.ZB2C_CHD_TFCT_BLKA': {"BLOCK_NAME is not null and BLOCK_NAME <> ''": 'Отклонение кол-ва записей с заполненным Названием блокировки превысило пороговое значение в МРФ ',
                                                 "BLOCK_TYPE_NAME is not null and BLOCK_TYPE_NAME <>''": 'Отклонение кол-ва записей с заполненным Названием типа блокировки превысило пороговое значение в МРФ ',
                                                 "BLOCK_STATUS_ID is not null and block_status_id <> 2": 'Отклонение кол-ва записей с заполненным Статусом блокировки превысило пороговое значение в МРФ '
                                                 },
                  'rtk_b2c.ZB2C_CHD_TFCT_BLKD': {"BLOCK_NAME is not null and BLOCK_NAME <> ''": 'Отклонение кол-ва записей с заполненным Названием блокировки превысило пороговое значение в МРФ ',
                                                 "BLOCK_TYPE_NAME is not null and BLOCK_TYPE_NAME <>''": 'Отклонение кол-ва записей с заполненным Названием типа блокировки превысило пороговое значение в МРФ ',
                                                 "BLOCK_STATUS_ID is not null and block_status_id <> 2": 'Отклонение кол-ва записей с заполненным Статусом блокировки превысило пороговое значение в МРФ '
                  },
                  'rtk_b2c.ZB2C_CHD_TFCT_CNTR': {"CNTRCT_ID is not null and CNTRCT_ID<>'' and lower(CNTRCT_ID) <>'не определено'":
                                                    'Отклонение кол-ва записей с заполненным id договора превысило пороговое значение в Мрф '},
                  'rtk_b2c.ZB2C_CHD_TFCT_KSCM': {'ALL_REV_SUM > 0':
                                                     'Отклонение кол-ва записей с общей выручкой по всем услугам за месяц > 0 превысило пороговое значение в Мрф '},
                  'rtk_b2c.ZB2C_CHD_TFCT_CST': {"FIO_CSTM is not null and FIO_CSTM<>'' and FIO_CSTM<>'Не определено'":
                                                'Отклонение кол-ва записей с заполненным Фио превысило пороговое значение',
                                                "BRTHD_CSTM is not null":
                                                'Отклонение кол-ва записей с заполненной датой рождения превысило пороговое значение'}
                  }

    param = {
        'rtk_b2c.ZB2C_CHD_TFCT_CLIENT_PROF': ['FIX_ON > 0',
                                              'BB_OPT_ON > 0',
                                              'BB_CPRM_ON > 0',
                                              'MVNO_ON > 0',
                                              'IPTV_ON > 0',
                                              'IPTV2_ON > 0'],
        'rtk_b2c.ZB2C_CHD_TFCT_AOPT': ["OPTN_NAME_FULL is not null and OPTN_NAME_FULL <> '' and OPTN_NAME_FULL <> 'Не определено'"],
        'rtk_b2c.ZB2C_CHD_TFCT_AMNT': ['Amount_all_shpd > 0',
                                       'amount_all_iptv > 0',
                                       'amount_all_ota > 0',
                                       'amount_all_mvno > 0'],
        'rtk_b2c.ZB2C_CHD_TFCT_ALLTPB': ["SRVS_TYPE is not null and SRVS_TYPE <> ''",
                                         'TP_BB_SPEED > 0',
                                         "TP_FULL is not null and TP_FULL <> ''"],
        'rtk_b2c.ZB2C_CHD_TFCT_ASRV': ["SRVS_TYPE is not null and SRVS_TYPE <> ''",
                                       "SRVS_TECH is not null and SRVS_TECH <> ''",
                                       'TRAF_IN_MC_TOTAL > 0',
                                       'TRAF_IN_OTA > 0',
                                       'TRAF_IN_VZ > 0',
                                       'TRAF_IN_MG > 0',
                                       'TRAF_IN_MN > 0',
                                       "SRVS_MEMBER_ID is not null and SRVS_MEMBER_ID <> ''"
                                       ],
        'rtk_b2c.ZB2C_CHD_TFCT_CST': ["SRVS_TYPE is not null and SRVS_TYPE <> ''",
                                      "SRVS_TECH is not null and SRVS_TECH <> ''",
                                      "FIO_CSTM is not null and FIO_CSTM <> ''",
                                      "BRTHD_CSTM is not null and BRTHD_CSTM <> ''"
                                      ],
        'rtk_b2c.ZB2C_CHD_TFCT_DH': ["HOUSE_TECH_FTTX is not null",
                                    "HOUSE_TECH_XPON is not null",
                                    "CITY_GID is not null and CITY_GID <> ''",
                                    "CITY_LID is not null and CITY_LID <> ''",
                                    # "CITY is not null and CITY <> ''", #понять почему этот лист битый
                                    "FULL_ADDRESS is not null and FULL_ADDRESS <> ''"],
        'rtk_b2c.ZB2C_CHD_TFCT_CNTR': ["CNTRCT_ID is not null and CNTRCT_ID<>'' and lower(CNTRCT_ID) <>'не определено'"],
        'rtk_b2c.ZB2C_CHD_TFCT_KSCM': ["ALL_REV_SUM > 0"]

    }

    err_cell = []

    writer = pd.ExcelWriter(f'AOPT.xlsx')
    for j in param_aopt:
        df = pd.DataFrame()
        for mrf in range(10, 18):
            # print(sql_shab_1.format(i, table[i], 'and ' + j, mrf))
            df1 = pd.read_sql(sql_shab_aopt.format(j, mrf), conn)
            try:
                if df1['Отклониение 1 среза от 2'].iloc[0] > 10:
                    err_cell.append(['rtk_b2c.ZB2C_CHD_TFCT_AOPT', j, mrf, df1['Отклониение 1 среза от 2'].iloc[0]])
            except:
                pass
            df = pd.concat([df1, df])
        df.to_excel(writer, sheet_name=j if len(j) < 20 else j[:20], index=False)
    writer.save()

    for i in table_one:
        writer = pd.ExcelWriter(f'{name_file[i]}.xlsx')
        for j in param[i]:
            df = pd.DataFrame()
            for mrf in range(10, 18):
                # print(sql_shab_1.format(i, table[i], 'and ' + j, mrf))
                if i != 'rtk_b2c.ZB2C_CHD_TFCT_CLIENT_PROF':
                    df1 = pd.read_sql(sql_shab_one_srav.format(i, table_one[i], j, mrf, 'table_mrf_id_not_centr'), conn)
                else:
                    df1 = pd.read_sql(sql_shab_one_srav.format(i, table_one[i], j, mrf, 'TABLE_mrf_id'), conn)
                try:
                    if df1['Отклониение 1 среза от 2'].iloc[0] > 10:
                        err_cell.append([i, j, mrf, df1['Отклониение 1 среза от 2'].iloc[0]])
                except:
                    pass
                df = pd.concat([df1, df])
            df.to_excel(writer, sheet_name=j if len(j) < 30 else j[:30], index=False)
        writer.save()


    writer_dmnt = pd.ExcelWriter('DMNT.xlsx')
    for i in param_dmnt:
        df = pd.DataFrame()
        for mrf in range(11, 18):
            df1 = pd.read_sql(sql_shab_dmnt.format(mrf, i), conn)
            if df1['Отклониение 1 среза от 2'].iloc[0] > 10:
                err_cell.append(['dmnt', i, mrf, df1['Отклониение 1 среза от 2'].iloc[0]])
            if df1['Отклонение сумм 1 среза от 2'].iloc[0] > 10:
                err_cell.append(['dmnt', i + '_sum', mrf, df1['Отклониение 1 среза от 2'].iloc[0]])
            df = pd.concat([df1, df])
        df.to_excel(writer_dmnt, sheet_name=i if len(i) < 30 else i[:31], index=False)
    writer_dmnt.save()


    writer_dopt = pd.ExcelWriter('DOPT.xlsx')
    for i in param_dopt:
        df = pd.DataFrame()
        for mrf in range(10, 18):
            df1 = pd.read_sql(sql_shab_dopt.format(mrf, i), conn)
            try:
                if df1['Отклониение 1 среза от 2'].iloc[0] > 10:
                    err_cell.append(['dopt', i, mrf, df1['Отклониение 1 среза от 2'].iloc[0]])
            except:
                pass
            df = pd.concat([df1, df])
        df.to_excel(writer_dopt, sheet_name=i if len(i) < 30 else i[:31], index=False)
    writer_dopt.save()

    # j = 'rtk_b2c.ZB2C_CHD_TFCT_BLKD'
    # writer_blk = pd.ExcelWriter(f'BLKD.xlsx')
    # for i in param_blk:
    #     df = pd.read_sql(sql_shab_blk.format(j, j, i), conn)
    #     for _, k in df.iterrows():
    #         try:
    #             if k[8] > 25:
    #                 err_cell.append([j, i, k[1], k[8]])
    #         except:
    #             print(traceback.format_exc())
    #     df.to_excel(writer_blk, sheet_name=i if len(i) < 30 else i[:31], index=False)
    # writer_blk.save()

    j = 'rtk_b2c.ZB2C_CHD_TFCT_BLKA'
    writer_blk = pd.ExcelWriter(f'BLKA.xlsx')
    for i in param_blk:
        df = pd.read_sql(sql_shab_blka.format(j, j, i), conn)
        for _, k in df.iterrows():
            try:
                if k[8] > 25:
                    err_cell.append([j, i, k[1], k[8]])
            except:
                print(traceback.format_exc())
        df.to_excel(writer_blk, sheet_name=i if len(i) < 30 else i[:31], index=False)
    writer_blk.save()

    writer_cst = pd.ExcelWriter('CST.xlsx')
    for i in param_cst:
        df = pd.read_sql(sql_shab_cst.format(i, i[:8]), conn)
        for _, k in df.iterrows():
            try:
                if k[7] > 10:
                    err_cell.append(['rtk_b2c.ZB2C_CHD_TFCT_CST', i, '', df1['Отклониение 1 среза от 2'].iloc[0]])
            except:
                pass
        df.to_excel(writer_cst, sheet_name=i if len(i) < 30 else i[:31], index=False)
    writer_cst.save()


    writer_amnt = pd.ExcelWriter('AMNT.xlsx')
    for i in param['rtk_b2c.ZB2C_CHD_TFCT_AMNT']:
        df = pd.DataFrame()
        for mrf in range(10, 18):
            df1 = pd.read_sql(sql_shab_amnt.format(i[:-4], i, mrf), conn)
            try:
                if df1['Отклониение 1 среза от 2'].iloc[0] > 10:
                    err_cell.append(['rtk_b2c.ZB2C_CHD_TFCT_AMNT', i, mrf, 0, df1['Отклониение 1 среза от 2'].iloc[0]])
                # if df1['Отклониение 1 среза от 3'].iloc[0] > 10:
                #     err_cell.append(['rtk_b2c.ZB2C_CHD_TFCT_AMNT', i, mrf, 0])
                if df1['Отклониение 1 среза cумм от 2'].iloc[0] > 10:
                    err_cell.append(['rtk_b2c.ZB2C_CHD_TFCT_AMNT', i, mrf, 1, df1['Отклониение 1 среза от 2'].iloc[0]])
                # if df1['Отклониение 1 среза cумм от 3'].iloc[0] > 10:
                #     err_cell.append(['rtk_b2c.ZB2C_CHD_TFCT_AMNT', i, mrf, 1])
            except:
                pass
            df = pd.concat([df1, df])
        df.to_excel(writer_amnt, sheet_name=i if len(i) < 30 else i[:31], index=False)
    writer_amnt.save()


    writer_itm = pd.ExcelWriter(f'ITM.xlsx')
    df = pd.read_sql(sql_shab_itm, conn)
    for _, k in df.iterrows():
            if k[8] > 10:
                err_cell.append(['itm', 'Отклонение кол-ва уникальных нлс превысило пороговое значение в МРФ', k[1], k[8]])
            pass
    df.to_excel(writer_itm, index=False)
    writer_itm.save()

    o = win32com.client.Dispatch("Outlook.Application")

    Msg = o.CreateItem(0)
    Msg.To = """Юрин Сергей Игоревич <yurin.sergey@south.rt.ru>;
    Ковалев Сергей Владимирович <Sergey.Kovalev@rt.ru>;
    Ситолик Никита Александрович <Nikita.Sitolik@rt.ru>;
    Буженик Татьяна Ивановна <BuzhenikTI@rt.ru>;
    Легеньков Павел Евгеньевич <legenkov-pe@ural.rt.ru>;
    mikhail.siukhin@south.rt.ru;
    support_dmcmb2c@rt.ru"""
    # Msg.To = "mikhail.siukhin@south.rt.ru"

    # Msg.CC = "more email addresses here"
    # Msg.BCC = "more email addresses here"

    Msg.Subject = "Проверка качества данных"
    body = """Добрый день!
Ниже предстален список витрин и проверок которые они не прошли
Проверки состоят из сравнения кол-ва уникальных нлс и/или сравнения средних начислений за определённый срез или промежуток
С полной информацией по витринам(% отклонений, даты срезов, значение статистик) можно ознакомиться в файлах во вложении\n"""
    name_1 = ''
    for i in err_cell:
        print(i)
        try:
            if name_1 != i[0]:
                body += '\n' + i[0] + '\n'
                name_1 = i[0]
            if i[0] == 'rtk_b2c.ZB2C_CHD_TFCT_AMNT':
                if i[3] == 0:
                    print(i)
                    body += param_text[i[0]][i[1]][0] + f"{i[2]}" + f' ({round(i[4], 2)} %)\n'
                else:
                    body += param_text[i[0]][i[1]][1] + f"{i[2]}" + f' ({round(i[4], 2)} %)\n'
            elif i[0] == 'itm':
                body += i[1] + ' ' + str(int(i[2])) + f' ({round(i[3], 2)} %)\n'
            else:
                body += param_text[i[0]][i[1]] + f"{i[2]}" + f' ({round(i[3], 2)} %)\n'
        except:
            continue
    Msg.Body = body

    attachment1 = os.getcwd() + r"\DMNT.xlsx"
    attachment2 = os.getcwd() + r"\DOPT.xlsx"
    attachment3 = os.getcwd() + r"\ALLTPB.xlsx"
    attachment4 = os.getcwd() + r"\AMNT.xlsx"
    attachment5 = os.getcwd() + r"\AOPT.xlsx"
    attachment6 = os.getcwd() + r"\ASRV.xlsx"
    attachment7 = os.getcwd() + r"\CLIENT_PROF.xlsx"
    attachment8 = os.getcwd() + r"\DH.xlsx"
    attachment9 = os.getcwd() + r"\ITM.xlsx"
    attachment10 = os.getcwd() + r"\BLKA.xlsx"
    # attachment11 = os.getcwd() + r"\BLKD.xlsx"
    attachment12 = os.getcwd() + r"\CNTR.xlsx"
    attachment13 = os.getcwd() + r"\KSCM.xlsx"
    attachment14 = os.getcwd() + r"\CST.xlsx"

    Msg.Attachments.Add(attachment1)
    Msg.Attachments.Add(attachment2)
    Msg.Attachments.Add(attachment3)
    Msg.Attachments.Add(attachment4)
    Msg.Attachments.Add(attachment5)
    Msg.Attachments.Add(attachment6)
    Msg.Attachments.Add(attachment7)
    Msg.Attachments.Add(attachment8)
    Msg.Attachments.Add(attachment9)
    Msg.Attachments.Add(attachment10)
    # Msg.Attachments.Add(attachment11)
    Msg.Attachments.Add(attachment12)
    Msg.Attachments.Add(attachment13)
    Msg.Attachments.Add(attachment14)

    Msg.Send()


def load_blka(conn_h, conn_GP):
    sql_cur_date = 'select max(add_days(to_date(snapshot_dt), -7)) from rtk_b2c.ZB2C_CHD_TFCT_BLKA'

    cur_date = conn_h.execute(sql_cur_date).fetchall()

    sql = "select 'rtk_b2c.ZB2C_CHD_TFCT_BLKA' as name, MRF_ID, count(*) as cnt, max(snapshot_dt) as max_st, min(snapshot_dt) as min_st from" \
          " edw_dmcm.tfct_blka where date(snapshot_dt) = '" + str(
        cur_date[0][0]) + "' and BLOCK_STATUS_ID is not null and block_status_id <> 2" \
                          "  group by mrf_id;"

    try:
        conn_h.execute("drop table msiukhin.ZB2C_CHD_TFCT_BLKA_week")
    except Exception as e:
        print(e)

    df1 = pd.read_sql(sql, conn_GP)
    df1.to_sql(name='ZB2C_CHD_TFCT_BLKA_WEEK', con=conn_h, schema='msiukhin', if_exists='append', index=False)


def save_sct(conn):
    sql = """delete from msiukhin.table_stat_cst;

insert into msiukhin.table_stat_cst                              
select 'CST' as name, count(*) as cnt, min(eff_dttm) as max_st, max(eff_dttm) as min_st, 'FIO_CSTM' as stat
from rtk_b2c.ZB2C_CHD_TFCT_CST
where FIO_CSTM is not null and FIO_CSTM<>'' and FIO_CSTM<>'Не определено'
;

insert into msiukhin.table_stat_cst                               
select 'CST' as name, count(*) as cnt, min(eff_dttm) as max_st, max(eff_dttm) as min_st, 'BRTHD_CS' as stat
from rtk_b2c.ZB2C_CHD_TFCT_CST
where BRTHD_CSTM is not null
;
    """
    for i in sql.split('\n\n'):
        conn.execute(i)


def check_table_week():
    log, passw = hanaLogin, hanaPassw
    if datetime.datetime.today().weekday() == 6:
        engine_HDB = create_engine('hana://{0}:{1}@10.42.40.60:30015'.format(log, passw))
        conn_hana = engine_HDB.connect()

        # GPlogin = 'mikhail.siukhin'
        # GPpassw = '07Yy4eBurek!'
        engine_GP = create_engine('postgresql://{0}:{1}@10.42.100.64:5432/edw_prod'.format(GPlogin, GPpassw))
        conn_gp = engine_GP.connect()

        # load_blka(conn_hana, conn_gp)
        update_files(conn_hana)
        save_sct(conn_hana)


if __name__ == '__main__':
    check_table_week()