import zerorpc
import gevent
from config import Config
import time
import sys

job_name = 0
input_file = ""
split_size = 0


c = zerorpc.Client()
c.connect(Config.MASTER_IP)
gevent.spawn(c.submit_job, job_name, input_file, split_size)

while True:
    time.sleep(2)
    progress = c.progress_update()
    print progress + "\n"
    if(progress == "Completed"):
        break


print "........... {0} Compeleted .........".format(job_name)

