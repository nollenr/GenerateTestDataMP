from multiprocessing import Queue, current_process
from platform import java_ver
import cockroach_manager
import time
import psycopg2

class MPQueue():
    def __init__(self, application_name = 'IPS', update_rec_with_leaseholder=False, unique_index_value_offset=0):
        self.update_rec_with_leaseholder = update_rec_with_leaseholder
        self.application_name = application_name
        self.unique_index_value_offset = unique_index_value_offset
        self.task_queue = Queue()
        self.done_queue = Queue()

        self.driver_task_queue = Queue()
        self.driver_done_queue = Queue()

    def worker(self, input, output):
        # This is all queue setup.  The queue processing starts at the "for args" loop.
        print('worker {} connecting to the database.'.format(current_process().name))
        try:
            crdb = cockroach_manager.CockroachManager.use_secret(True)
        except:
            print('Unable to connect to the database')
            exit(1)

        cursor = crdb.connection.cursor()
        cursor.execute('select crdb_internal.node_id(), gateway_region()')
        cluster_node, gateway_region = cursor.fetchone()
        # print('node: {}\tgateway_region: {}'.format(cluster_node, gateway_region))
        cursor.execute('SET application_name = %s',(self.application_name,))
        print('worker {} connected to the cluster on node {} gateway region {}.'.format(current_process().name, cluster_node, gateway_region))

        if self.update_rec_with_leaseholder:
            sql_statement = 'insert into ips(worker, cluster_node, gateway_region, int8_col, varchar50_col, bool_col, jsonb_col) values (%s, %s, %s, %s, %s, %s, %s) returning id'
        else:
            sql_statement = 'insert into ips(worker, cluster_node, gateway_region, int8_col, varchar50_col, bool_col, jsonb_col) values (%s, %s, %s, %s, %s, %s, %s)'

        print('Start the processing of the queue...')
        for args in iter(input.get, 'STOP'):
            values = (current_process().name,cluster_node,gateway_region,args[0]+self.unique_index_value_offset,'one','True','{"one": "1", "two": "2"}')
            tic = time.perf_counter()
            cursor.execute(sql_statement, values)
            toc = time.perf_counter()
            # result = 'from worker: {}\tthe args are: {}'.format(current_process().name, args)
            if self.update_rec_with_leaseholder:
                id = cursor.fetchone()
                try:
                    cursor.execute("select lease_holder from [show range from table ips for row(%s)]", id)
                    lease_holder = cursor.fetchone()[0]
                    cursor.execute('update ips set lease_holder = %s where id = %s',(lease_holder, id))
                except (Exception, psycopg2.DatabaseError) as error:
                    print("Error getting the lease_holder", error)    
            output.put({'worker':current_process().name, 'insert': toc-tic, 'records': 1})
        # print('Worker {} is done.'.format(current_process().name))

    def enqueuer(self, intput, outout):
        for args in iter(input.get, 'STOP'):
            pass

    def dequeuer(self, intput, output):
        for args in iter(input.get, 'STOP'):
            pass