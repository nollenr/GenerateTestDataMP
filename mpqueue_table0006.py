from multiprocessing import Process, Queue, current_process
from platform import java_ver
import cockroach_manager
import time
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.errors import SerializationFailure
import random
import string

"""
In this version, the table will be table0006
Only multiple rows inserts
No returning
"""

"""
  CREATE TABLE public.table0006 (
      column1 UUID NOT NULL DEFAULT gen_random_uuid(),
      create_time TIMESTAMPTZ NOT NULL DEFAULT now():::TIMESTAMPTZ,
      region STRING NOT NULL DEFAULT crdb_internal.locality_value('region':::STRING),
      az STRING NOT NULL DEFAULT crdb_internal.locality_value('zone':::STRING),
      state STRING(2) NOT NULL,
      column_2 STRING NULL,
      column_3 STRING NULL,
      column_4 STRING NULL,
      column_5 STRING NULL,
      column_6 STRING NULL,
      column_7 STRING NULL,
      column_8 STRING NULL,
      column_9 STRING NULL,
      column_10 STRING NULL,
      column_11 STRING NULL,
      column_12 STRING NULL,
      column_13 STRING NULL,
      column_14 STRING NULL,
      column_15 STRING NULL,
      column_16 STRING NULL,
      column_17 STRING NULL,
      column_18 STRING NULL,
      column_19 STRING NULL,
      column_20 STRING NULL,
      column_21 STRING NULL,
      column_22 STRING NULL,
      column_23 STRING NULL,
      column_24 STRING NULL,
      column_25 STRING NULL,
      CONSTRAINT "primary" PRIMARY KEY (column1 ASC)
  );
"""

class MPQueue():
    """Create a worker definition that can be used in processing queues.

    Using a class with instance variables was the best way that I could find to send start up parameters to a worker.
    """
    def __init__(self, application_name = 'IPS', use_aws_secret=False, secret_name=None, region_name=None, auto_commit=False, connection_dict=None, update_rec_with_leaseholder=False, use_multi_row_insert = False):
        self.region_name = region_name
        self.auto_commit = auto_commit
        self.secret_name = secret_name
        self.use_multi_row_insert = True
        self.use_aws_secret = use_aws_secret
        self.connection_dict = connection_dict
        self.update_rec_with_leaseholder = False
        self.application_name = application_name
        self.task_queue = Queue()
        self.done_queue = Queue()

        self.data_task_queue = Queue()

    # Put data on the task_queue for the db_workers to process
    def create_data_worker(self, input, output, MUTLI_ROW_INSERT_SIZE):
        for arg in iter(input.get, 'STOP'):
            data = ()   
            for j in range(MUTLI_ROW_INSERT_SIZE):
                column_data = ''.join(random.choices(string.ascii_letters, k=128))
                #state,column_2,column_3,column_4,column_5,column_6,column_7,column_8,column_9,column_10,column_11,column_12,column_13,column_14,column_15,column_16,column_17,column_18,column_19,column_20,column_21,column_22,column_23,column_24,column_25
                row = ("CA",str(arg),column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,column_data,)
                # faker turns out to be way too slow
                # row = ("CA",str(i),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128),faker.text(max_nb_chars=128))            
                data = data + (row,)
            output.put(data)
            print ("\r Size of the db queue: {}".format(output.qsize()), end="", flush=True)
            while output.qsize() > 1000:
                # print('\tcreate_data_worker is creating too many tasks.  task queue size is over 1000 records.')
                time.sleep(0.5)
        print('create_data_worker is done putting stuff on queue.  Number of items on (database) task queue is {}'.format(output.qsize()))

    def db_worker(self, input, output):
        # This is all queue setup.  The queue processing starts at the "for args" loop.
        try:
            if self.use_aws_secret:
                crdb = cockroach_manager.CockroachManager.use_secret(self.secret_name, self.region_name, self.auto_commit) 
            else:
                crdb = cockroach_manager.CockroachManager(self.connection_dict, self.auto_commit)
        except:
            print('Unable to connect to the database')
            exit(1)

        cursor = crdb.connection.cursor()
        cursor.execute('select crdb_internal.node_id(), gateway_region()')
        cluster_node, gateway_region = cursor.fetchone()
        # print('node: {}\tgateway_region: {}'.format(cluster_node, gateway_region))
        cursor.execute('SET application_name = %s',(self.application_name,))
        print('\nworker {} connected to the cluster on node {} gateway region {}.'.format(current_process().name, cluster_node, gateway_region))

        sql_statement = 'insert into table0006(state,column_2,column_3,column_4,column_5,column_6,column_7,column_8,column_9,column_10,column_11,column_12,column_13,column_14,column_15,column_16,column_17,column_18,column_19,column_20,column_21,column_22,column_23,column_24,column_25) values '
        sql_statement += " %s"
        
        print('Start the processing of the queue...')
        for args in iter(input.get, 'STOP'):
            # based on anecdotal testing, I believe a cursor.execute is faster than execute_values for single row inserts
            # if there is only one arg has length one, then this is a single row insert.  
            # We're going to do a multi-row insert
            tic = time.perf_counter()
            execute_values(cursor, sql_statement, args)
            toc = time.perf_counter()
            # result = 'from worker: {}\tthe args are: {}'.format(current_process().name, args)
            output.put({'worker':current_process().name, 'insert': toc-tic, 'records': len(args)})
        # print('Worker {} is done.'.format(current_process().name))


        