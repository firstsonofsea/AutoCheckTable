from app import app, db
from flask import render_template, url_for, redirect
from app.models import Task, InfoRun
from .tasks import all_task
import datetime
import pandas as pd


@app.route('/')
@app.route('/index')
def index():
    task = Task.query.filter_by(hidden=False).all()
    return render_template('index.html', tasks=task)


@app.route("/forward/<task_name>", methods=['POST'])
def move_forward(task_name):
    for i in all_task:
        if i.name == task_name:
            try:
                task = Task.query.filter_by(name=task_name).first()
                i.started_task()
                # task = Task.query.filter_by(name=task_name).first()
                task.last_status = True
            except:
                task.last_status = False
            task.last_run = datetime.datetime.now()
            db.session.commit()
    return redirect(url_for('view_task', name=task.name))


@app.route('/task/<name>')
def view_task(name):
    task = Task.query.filter_by(name=name).first()
    runs = InfoRun.query.filter_by(task_id=task.id).all()
    info = {'name': task.name,
            'info': task.info,
            'last_run': task.last_run,
            'status': 'OK' if task.last_status else 'Error'}
    return render_template('task_info.html', task=info, runs=runs)


@app.route('/table_info')
def table_info():
    path = r'C:\Users\Mikhail.Siukhin\PycharmProjects\flask_sheduler\app\tasks\check_table\result.csv'
    df = pd.read_csv(path)
    itog = []
    for _, i in df.iterrows():
        row = {
            'name': i[0],
            'date_old': i[1],
            'count_old': i[2],
            'date_act': i[3],
            'count_act': i[4],
            'delta': i[5]
        }
        itog.append(row)
    return render_template('table_info.html', info=itog)