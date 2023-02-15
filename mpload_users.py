from multiprocessing import Process, Queue, current_process
from platform import java_ver
import cockroach_manager
import time
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.errors import SerializationFailure
import faker
import json

class MPQueue():
    """Create a worker definition that can be used in processing queues.

    Using a class with instance variables was the best way that I could find to send start up parameters to a worker.

    create database db_with_abstractions_limitations;
    alter database db_with_abstractions_limitations set primary region "aws-us-west-2";
    alter database db_with_abstractions_limitations add region "aws-ap-southeast-1";
    alter database db_with_abstractions_limitations add region "aws-eu-central-1";

    CREATE TABLE public.users (
        id UUID NOT NULL DEFAULT gen_random_uuid(),
        auth_id VARCHAR(100) NOT NULL,
        first_name VARCHAR(50) NOT NULL,
        last_name VARCHAR(50) NULL,
        email VARCHAR NOT NULL,
        profile_picture_id VARCHAR NULL,
        default_picture VARCHAR NULL,
        preferences JSONB NULL,
        metadata JSONB NULL,
        national_id STRING NOT NULL,
        created_at TIMESTAMPTZ NULL DEFAULT now():::TIMESTAMPTZ,
        updated_at TIMESTAMPTZ NULL DEFAULT now():::TIMESTAMPTZ,
        crdb_region db_with_abstractions_limitations.public.crdb_internal_region NOT VISIBLE NOT NULL DEFAULT default_to_database_primary_region(gateway_region())::db_with_abstractions_limitations.public.crdb_internal_region,
        CONSTRAINT users_rbr_pkey PRIMARY KEY (id ASC)
    ) LOCALITY REGIONAL BY ROW;


    """
    def __init__(self, application_name = 'load_users', use_aws_secret=False, secret_name=None, region_name=None, auto_commit=False, connection_dict=None, update_rec_with_leaseholder=False, use_multi_row_insert = False):
        self.region_name = region_name
        self.auto_commit = auto_commit
        self.secret_name = secret_name
        self.use_multi_row_insert = use_multi_row_insert
        self.use_aws_secret = use_aws_secret
        self.connection_dict = connection_dict
        self.update_rec_with_leaseholder = update_rec_with_leaseholder
        self.application_name = application_name
        self.task_queue = Queue()
        self.done_queue = Queue()

        self.driver_task_queue = Queue()
        self.driver_done_queue = Queue()
        self.fake = faker.Faker()


    def worker(self, input, output):
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
        print('worker {} connected to the cluster on node {} gateway region {}.'.format(current_process().name, cluster_node, gateway_region))

        sql_statement = 'insert into users(auth_id, first_name, last_name, email, profile_picture_id, default_picture, national_id) values '

        if self.use_multi_row_insert:
            sql_statement += " %s"
        else:
            sql_statement += " (%s, %s, %s, %s, %s, %s, %s)"

        if self.update_rec_with_leaseholder:
            sql_statement += ' returning id'

        print('Start the processing of the queue...')
        for args in iter(input.get, 'STOP'):
            #         auth_id,               first_name,  last_name,     email,               profile_picture_id, default_picture, national_id
            values = [current_process().name,cluster_node,gateway_region,'myemail@gmail.com', 'profile_picture','default picture', self.fake.ssn()]

            # based on anecdotal testing, I believe a cursor.execute is faster than execute_values for single row inserts
            # if there is only one arg has length one, then this is a single row insert.  
            if (len(args)) == 1:
                tic = time.perf_counter()
                cursor.execute(sql_statement, values)
                toc = time.perf_counter()
            else:
                # We're going to do a multi-row insert
                values = [(values) for arg in args]    
                # change the last value in the list (the ssn) to a new ssn, so that all records have a unique ssn
                values = [value[:-1] + [self.fake.ssn()] for value in values]
                tic = time.perf_counter()
                execute_values(cursor, sql_statement, values)
                toc = time.perf_counter()
                # result = 'from worker: {}\tthe args are: {}'.format(current_process().name, args)
            output.put({'worker':current_process().name, 'insert': toc-tic, 'records': 1})
        # print('Worker {} is done.'.format(current_process().name))


        