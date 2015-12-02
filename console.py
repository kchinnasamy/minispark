import zerorpc
import gevent
from config import Config
import time

c = zerorpc.Client()
c.connect(Config.MASTER_IP)

while True:
    cmd = raw_input('>  ')
    respose = c.submit_cmd(cmd)
    while True:
        gevent.sleep(0)
        respose = c.progress_update()
        if respose != "Wait":
            break
    if respose == "Complete":
        print c.final_output()
    if cmd == "q":
        break

        # a = RDD(1)
        # b = a.textFile('w',5)
        # b.count()
