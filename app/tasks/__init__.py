import traceback

import schedule
from app.models import Task, InfoRun
from app import db
from datetime import datetime


def start_sch():
    pass


class CustomTask():
    def __init__(self, name, info, exec_func, scheduler_shab):
        self.name = name
        self.info = info
        self.exec_func = exec_func
        self.dbTask = Task.query.filter_by(name=name).first()
        if self.dbTask is None:
            self.dbTask = Task(name=self.name,
                                info=self.info,
                                )
            db.session.add(self.dbTask)
            db.session.commit()
        self.schedule_func = scheduler_shab.do(self.started_task)
        schedule.cancel_job(scheduler_shab)

    def started_task(self):
        print("{} :starf {}".format(datetime.now(), self.name))
        id_task = self.dbTask.id
        try:
            # self.schedule_func.run()
            self.exec_func()
            self.dbTask.last_status = True
            ir = InfoRun(task_id=id_task, date=datetime.now(), status=True, info='Выполнено без ошибок')
        except:
            print(traceback.format_exc())
            self.dbTask.last_status = False
            ir = InfoRun(task_id=id_task, date=datetime.now(), status=False, info=traceback.format_exc())
        self.dbTask.last_run = datetime.now()
        db.session.add(ir)
        db.session.commit()
        print("{} :end {}".format(datetime.now(), self.name))


from .load_delivery.main import load_delivery
from .load_comp_key.main import load_comp_key
from .sale_available.main import sale_available
from .test_task.main import test
from .check_table.main import check_table
from .call_proc.main import call_proc
from .week_update.main import week_upadate
from .create_key_exeption_main.main import create_key_exeptio_main
from .check_table.test_check import check_table_week
from .check_max_eff_dttm.main import check_max_eff_dttm
from .update_auto_podb.main import call_auto_podb

all_task = [
    CustomTask('load_delivery', 'test',
               load_delivery, schedule.every().days.at('08:00').do(start_sch).tag('update_key')),
    CustomTask('load_comp_key', 'test',
               load_comp_key, schedule.every().days.at('06:00').do(start_sch).tag('update_key')),
    CustomTask('sale_available', 'test',
               sale_available, schedule.every().day.at('08:30').do(start_sch).tag('update_key')),
    CustomTask('test', 'test',
               test, schedule.every(3600*24).seconds.do(start_sch).tag('update_key')),
    # CustomTask('check_table', 'Ежедневная проверка кол-ва строк в таблице\n можно посмотреть информацию на этом сайте',
    #            check_table, schedule.every().day.at('08:15').do(start_sch).tag('custom_func')),
    CustomTask('call_proc', 'запуск процедуры проверки itm_exeption',
                call_proc, schedule.every(3600).seconds.do(start_sch).tag('update_key')),
    CustomTask('week_update', 'процедура с обновлением еженедельных кючей',
               week_upadate, schedule.every().days.at('08:00').do(start_sch).tag('update_key')),
    CustomTask('update_exeption_main', 'процедура с обновлением ключа exeption_main',
               create_key_exeptio_main, schedule.every().day.at('04:00').do(start_sch).tag('update_key')),
    CustomTask('check_table_week', 'test',
               check_table_week, schedule.every().days.at('17:03').do(start_sch).tag('mailing_list')),
    CustomTask('check_max_eff_dttm', 'проверка времени обноления таблиц по мрф',
               check_max_eff_dttm, schedule.every().day.at('06:00').do(start_sch).tag('check_table')),
    CustomTask('call_auto_podb', 'обновление ключей автоподбора',
               call_auto_podb, schedule.every().day.at('04:00').do(start_sch).tag('update_key'))
    ]

all_task_in_bd = Task.query.all()

for i in all_task_in_bd:
    flag = True
    for j in all_task:
        if i.name == j.name:
            flag = False
    i.hidden = flag

db.session.commit()