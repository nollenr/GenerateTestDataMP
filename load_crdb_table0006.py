from http.client import MULTI_STATUS
from pickle import FALSE
import mpqueue_table0006
from multiprocessing import Process, Queue, current_process
import random
import string
import time

"""
In this version, the table will be table0006
Only multiple rows inserts
No returning
"""

"""
Table used in this load experiment
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

if __name__ == '__main__':

    NUMBER_OF_DB_WORKERS=24
    NUMBER_OF_CREATE_DATA_WORKERS=5
    NUMBER_OF_TASKS=10000000
    INCLUDE_LEASEHOLDER = False
    USE_UNIQUE_INDEX = False
    USE_MULTI_ROW_INSERT = True
    MUTLI_ROW_INSERT_SIZE = 100
    AUTO_COMMIT = True
    # Database connection details will either be from an AWS Secret, or they'll have
    # to be supplied as a connection dictionary
    GET_DATABASE_CONNECTION_DETAILS_FROM_AWS_SECRET = False
    SECRET_NAME="/nollen/nollen-cmek-cluster"
    REGION_NAME="us-west-2"
    MAX_DATA_QUEUE_SIZE=100000
    MAX_DB_QUEUE_SIZE=1000

    # The Cockroach Manager Class allows connection via secret manager or a connection string.
    # To use a connection string, complete the details below (you can place the password in an
    # environment variable).  Use something like the following:
    """
    try:
        crdb_password = os.environ['mypass']
        # print('crdb_password: {}'.format(crdb_password))
    except:
        print('the "mypass" environment variable has not been set.')
        exit(1)
    """

    if GET_DATABASE_CONNECTION_DETAILS_FROM_AWS_SECRET:
        connect_dict = None
    else:
        # Example using certs to connect
        
        """
        connect_dict = {
            "user"          : "login",
            "host"          : "cockroach-dev.klei.com",
            "port"          : "26257",
            "dbname"        : "nvidia",
            "sslmode"       : "require",
            "sslrootcert"   : "/home/ec2-user/Library/CockroachCloud/certs/klei-demo-ca.crt",
            "sslcert"       : "/home/ec2-user/Library/CockroachCloud/certs/klei-client-login.crt",
            "sslkey"        : "/home/ec2-user/Library/CockroachCloud/certs/klei-client-login.key"
        }
        """

        """
        Note, that if the password is not supplied, the connection manager will look
        at the os envriornment for "password"

        Use something like the following:
            export HISTCONTROL=ignorespace
                export password=password
        """
        connect_dict = {
            "user": "ron",
            "password": "ron123",
            "host": "nollen-nvidia-nlb-6780e48e229ffebc.elb.us-west-2.amazonaws.com",
            "port": "26257",
            "dbname": "nvidia",
            "sslrootcert": "/home/ec2-user/Library/CockroachCloud/certs/ca.crt"
            }

    # Initialize the multiprocesssing class so that the worker can be started and passed execution parameters.
    mpunit = mpqueue_table0006.MPQueue(application_name = 'table0006', use_aws_secret = GET_DATABASE_CONNECTION_DETAILS_FROM_AWS_SECRET, secret_name=SECRET_NAME, region_name=REGION_NAME, auto_commit=AUTO_COMMIT, connection_dict=connect_dict, update_rec_with_leaseholder=INCLUDE_LEASEHOLDER, use_multi_row_insert=USE_MULTI_ROW_INSERT)

    for i in range(NUMBER_OF_DB_WORKERS):
        Process(target=mpunit.db_worker, args=(mpunit.task_queue, mpunit.done_queue)).start()
    print('DB Workers are initialized')

    for i in range(NUMBER_OF_CREATE_DATA_WORKERS):
        Process(target=mpunit.create_data_worker, args=(mpunit.data_task_queue, mpunit.task_queue, MUTLI_ROW_INSERT_SIZE)).start()
    print('Data Workers are initialized')

    for i in range(NUMBER_OF_TASKS):
        mpunit.data_task_queue.put((i,),)
        print ("\r Size of the data task queue (in main): {}\tCurrent Task Number: {}".format(mpunit.data_task_queue.qsize(),i), end="", flush=True)
        if mpunit.data_task_queue.qsize() > MAX_DATA_QUEUE_SIZE:
            # print('task queue size is over {} records.'.format(MAX_DATA_QUEUE_SIZE))
            time.sleep(0.1)
    print('The main program is done putting stuff on queue for the data workers.  Number of items on task queue is {}'.format(mpunit.data_task_queue.qsize()))

    # for i in range(NUMBER_OF_CREATE_DATA_WORKERS):
    #     mpunit.data_task_queue.put('STOP')
    # print('STOP put on the data task queue')

    for i in range(NUMBER_OF_TASKS):
        # mpunit.done_queue.get()
        print('\t', mpunit.done_queue.get())
    print('done getting stuff from queue.  Number of items on done queue is {}'.format(mpunit.done_queue.qsize()))


    for i in range(NUMBER_OF_DB_WORKERS):
        mpunit.task_queue.put('STOP')