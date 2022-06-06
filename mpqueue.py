from multiprocessing import Process, Queue, current_process
from platform import java_ver
import cockroach_manager
import time
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.errors import SerializationFailure

class MPQueue():
    """Create a worker definition that can be used in processing queues.

    Using a class with instance variables was the best way that I could find to send start up parameters to a worker.
    """
    def __init__(self, application_name = 'IPS', use_aws_secret=False, connection_dict=None, update_rec_with_leaseholder=False, use_multi_row_insert = False):
        self.use_multi_row_insert = use_multi_row_insert
        self.use_aws_secret = use_aws_secret
        self.connection_dict = connection_dict
        self.update_rec_with_leaseholder = update_rec_with_leaseholder
        self.application_name = application_name
        self.task_queue = Queue()
        self.done_queue = Queue()

        self.driver_task_queue = Queue()
        self.driver_done_queue = Queue()


    def worker(self, input, output):
        # This is all queue setup.  The queue processing starts at the "for args" loop.
        try:
            if self.use_aws_secret:
                crdb = cockroach_manager.CockroachManager.use_secret(True) 
            else:
                crdb = cockroach_manager.CockroachManager(self.connection_dict)
        except:
            print('Unable to connect to the database')
            exit(1)

        cursor = crdb.connection.cursor()
        cursor.execute('select crdb_internal.node_id(), gateway_region()')
        cluster_node, gateway_region = cursor.fetchone()
        # print('node: {}\tgateway_region: {}'.format(cluster_node, gateway_region))
        cursor.execute('SET application_name = %s',(self.application_name,))
        print('worker {} connected to the cluster on node {} gateway region {}.'.format(current_process().name, cluster_node, gateway_region))

        sql_statement = 'insert into ips(worker, cluster_node, gateway_region, int8_col, varchar50_col, bool_col, jsonb_col) values '
        if self.use_multi_row_insert:
            sql_statement += " %s"
        else:
            sql_statement += " (%s, %s, %s, %s, %s, %s, %s)"

        if self.update_rec_with_leaseholder:
            sql_statement += ' returning id'
        

        print('Start the processing of the queue...')
        for args in iter(input.get, 'STOP'):
            # TODO
            # This no longer works in a multi-row insert - actually, it kinda does!
            # Only 90% of the int8_col values should be null.  10% have values.
            if args[0][0]%10:
                int8_val = None
            else:
                int8_val = args[0][0]
            values = [current_process().name,cluster_node,gateway_region,int8_val,'one-hundred','True','{"one": "1", "two": "2"}']

            # based on anecdotal testing, I believe a cursor.execute is faster than execute_values for single row inserts
            # if there is only one arg has length one, then this is a single row insert.  
            if (len(args)) == 1:
                tic = time.perf_counter()
                cursor.execute(sql_statement, values)
                toc = time.perf_counter()
            else:
                # We're going to do a multi-row insert
                values = [(values) for arg in args]    
                tic = time.perf_counter()
                execute_values(cursor, sql_statement, values)
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

    def enqueuer(self, input, outout):
        for args in iter(input.get, 'STOP'):
            for i in range(args[0]):
                Process(target=self.worker, args=(self.task_queue, self.done_queue)).start()
            for i in range(args[1]):
                self.task_queue.put((i,i))
                if self.task_queue.qsize() > 10000:
                #     print('task queue size is over 10000 records.')
                    time.sleep(0.1)
            print('done putting stuff on queue.  Number of items on task queue is {}'.format(mpunit.task_queue.qsize()))

    def dequeuer(self, intput, output):
        for args in iter(input.get, 'STOP'):
            pass
        # When everything has been dequeued, stop the workers
        