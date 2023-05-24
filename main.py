from app import app
from app.tasks import schedule
import time
from multiprocessing import Process


def run_schedule():
    while 1:
        schedule.run_pending()
        time.sleep(1)


if __name__ == '__main__':
    t = Process(target=run_schedule)
    t.start()
    app.run(host='10.144.2.227', port=8082)
