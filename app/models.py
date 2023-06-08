from app import db
from datetime import datetime


class Task(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(64), index=True, unique=True)
    last_run = db.Column(db.DateTime, default=datetime.utcnow)
    last_status = db.Column(db.Boolean, default=False)
    info = db.Column(db.String(512))
    hidden = db.Column(db.Boolean, default=False)
    logs = db.relationship('InfoRun', backref='task', lazy='dynamic')

    def __repr__(self):
        return '<Task: {}, info: {}, last_run: {}, hidden: {}>'.format(self.name, self.info, self.last_run, self.hidden)


class InfoRun(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    task_id = db.Column(db.Integer, db.ForeignKey('task.id'))
    date = db.Column(db.DateTime, default=datetime.utcnow)
    status = db.Column(db.Boolean, default=False)
    info = db.Column(db.Text)


class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    Name = db.Column(db.String(64))
    cr_date = db.Column(db.DateTime, default=datetime.utcnow)
    role = db.Column(db.Boolean, default=False)
    info = db.Column(db.Text)