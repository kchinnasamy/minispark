import zerorpc
from config import Config
import gevent
import time
from collections import OrderedDict


class Master(object):
    action_methods = ["count", "collect"]
    progress = ""
    all_workers = set()
    workers_info = {}  # "id" : None, "current_job" : None, "compeleted_jobs": set(), "last_received_hb": xxx
    lineage_graph = OrderedDict()
    busy_workers = set()
    idle_workers = set()
    start_execution = False
    complete_output = ""

    def __init__(self):
        self.reset_for_new_job()
        # gevent.spawn(self.controller)
        gevent.spawn(self.validate_workers)
        pass

    def reset_for_new_job(self):
        Master.progress = ""

    def process_complete_task(self, ip_address, status, job_id, job_type, output):
        # Master.progress = self.get_printable_status(status)
        print"[Complete Task by {0} ....  Status - {1}, Job_type - {2}, Job_ID - {3}......... ] \n".format(
            ip_address, self.get_printable_status(status), self.get_printable_status(job_type), job_id)
        Master.progress = "Complete"
        Master.complete_output = output
        if status == Config.WORKER_STATUS_COMPLETE:
            self.set_worker_idle(ip_address)

    def process_heart_beat(self, ip_address, status, job_id, job_type, output):
        # Master.progress = self.get_printable_status(status)
        print"[Heart_beat_Recieved from {0} ....  Status - {1}, Job_type - {2}, Job_ID - {3}......... ] \n".format(
            ip_address, self.get_printable_status(status), self.get_printable_status(job_type), job_id)
        if status == Config.WORKER_STATUS_COMPLETE:
            self.set_worker_idle(ip_address)
        self.register_worker(ip_address)

    def get_printable_status(self, status):
        if status == Config.WORKER_STATUS_REDUCE_FAILED:
            return "Failed"
        elif status == Config.WORKER_STATUS_IDLE:
            return "Idle"
        elif status == Config.WORKER_STATUS_COMPLETE:
            return "Completed"
        elif status == Config.WORKER_STATUS_WORKING_JOB:
            return "Task"
        elif status == Config.WORKER_STATUS_WORKING_REDUCE:
            return "ReduceTask"
        elif status == Config.WORKER_STATUS_WORKING_SHUFFLE:
            return "ShuffleTask"
        else:
            return str(status)

    def reset_failed_worker_job(self, worker_id):
        pass

    def update_completed_jobs(self, ip_address, job_id):
        pass

    def validate_workers(self):
        # Check for worker heart beat Intervals.
        # Unregister invalid workers
        # reassign the job to the pending job pool
        while True:
            cur_time = time.time()
            for key in Master.workers_info.keys():
                value = Master.workers_info[key]
                last_time = value['last_received_hb']
                if cur_time - last_time > (Config.HEART_BEAT_TIME_INTREVAL * 3 + 1):
                    print '******************** Worker {0} is down ********************'.format(value['id'])
                    self.unregister_worker(key)

            gevent.sleep(Config.HEART_BEAT_TIME_INTREVAL)

    def register_worker(self, ip_address):
        cur_time = time.time()
        if ip_address in Master.workers_info:
            Master.workers_info[ip_address]['last_received_hb'] = cur_time
            return
        Master.workers_info[ip_address] = {"id": ip_address, "current_map_job": None, "compeleted_jobs": set(),
                                           "last_received_hb": cur_time, "current_reduce_job": None}
        Master.all_workers.add(ip_address)
        self.set_worker_idle(ip_address)
        print "[New worker {0} Registered .... ]".format(ip_address)

    def unregister_worker(self, ip_address):
        self.reset_failed_worker_job(ip_address)
        del Master.workers_info[ip_address]
        Master.all_workers.discard(ip_address)

    def start_job(self):
        worker_id = Master.idle_workers.pop()
        job_name = "Test Job"
        input_file = ""
        start_index = 0
        chunk_size = 0
        c = zerorpc.Client()
        c.connect(worker_id)
        print "[......... Starting {1} Job On {0} ..... (Map Phase) ......] \n".format(worker_id, job_name)

        if c.start_job(job_name, input_file, start_index, chunk_size, Master.no_of_reducers):
            print "[......... {1} Job started successfully On  {0} ..... Job_id - {2} (Map Phase).......] \n".format(
                worker_id, job_name, start_index)
            Master.workers_info[worker_id]["current_map_job"] = start_index
            self.set_worker_busy(worker_id)
        else:
            print "[.............. Unable to Start {1} Job On  {0} ..... (Map Phase)..........] \n".format(worker_id,
                                                                                                           job_name)
            Master.workers_info[worker_id]["current_map_job"] = None
            self.set_worker_idle(worker_id)

        c.close()

    def set_worker_busy(self, ip_address):
        Master.idle_workers.discard(ip_address)
        Master.busy_workers.add(ip_address)

    def set_worker_idle(self, ip_address):
        Master.busy_workers.discard(ip_address)
        Master.idle_workers.add(ip_address)

    def split_input(self, input_file, chunk_size):
        # return splitter.split_file(input_file, chunk_size)
        pass

    def submit_job(self, job_name, input_file, chunk_size):
        self.reset_for_new_job()
        Master.progress = "Submiting Job ....."
        Master.job_name = job_name
        Master.input_file = input_file
        Master.chunk_size = chunk_size
        # Master.split_map_jobs = self.split_input(input_file, chunk_size)
        # Master.pending_map_jobs = set(Master.split_map_jobs)
        # Master.pending_reduce_jobs = set(list(range(0, no_of_reducers)))
        print "[{0} Job Submmited ..... ]".format(job_name)

    def get_job_id(self, job):
        return Master.split_map_jobs.index(job)

    def get_other_workers(self, ip_address):
        other_workers = []
        for worker_id in Master.all_workers:
            if worker_id != ip_address:
                other_workers.append(worker_id)
        return other_workers

    def progress_update(self):
        return Master.progress

    def final_output(self):
        Master.progress = "Idle"
        return Master.complete_output

    def do_job(self):
        worker_id = Master.idle_workers.pop()
        c = zerorpc.Client()
        c.connect(worker_id)
        rdd_name = "action"
        self.set_worker_busy(worker_id)
        return c.start_job(Master.lineage_graph, rdd_name, Master.lineage_graph[rdd_name])
        c.close()

    def submit_cmd(self, cmd):
        self.lineage_builder(cmd)
        if Master.start_execution:
            self.do_job()
            Master.progress = "Wait"
            return Master.progress
        return Master.lineage_graph

    def lineage_builder(self, input_cmds):
        cmds = input_cmds.split("\n")
        for cmd in cmds:
            stmt = cmd.split("=")
            var_name = stmt[0].strip()
            rhs = stmt[1].split(".", 1)
            if len(rhs) == 1:
                Master.lineage_graph[var_name] = {"parent": "head",
                                                  "func": self.parse_func(rhs[0].strip())}
            else:
                func_info = self.parse_func(rhs[1].strip())
                if Master.start_execution:
                    Master.lineage_graph["action"] = {"parent": rhs[0].strip(),
                                                      "func": func_info}
                else:
                    Master.lineage_graph[var_name] = {"parent": rhs[0].strip(),
                                                      "func": func_info}

    def parse_func(self, func):
        Master.start_execution = False
        func_data = func.split("(", 1)
        func_name = func_data[0]
        if func_name in Master.action_methods:
            Master.start_execution = True
        func_params = func_data[1].rsplit(")", 1)[0]
        params = ""
        if len(func_params) > 0:
            params = func_params

        return {"func_name": func_name, "func_params": params, "dependency": "N"}


if __name__ == '__main__':
    s = zerorpc.Server(Master())
    print(s.bind(Config.MASTER_IP))
    s.run()
