from multiprocessing import Process, Queue, current_process
from platform import java_ver
import cockroach_manager
import time
import psycopg2
from psycopg2.extras import execute_values
from psycopg2.errors import SerializationFailure
import faker
import random
import json
import uuid

class MPQueue():
    """Create a worker definition that can be used in processing queues.

    Using a class with instance variables was the best way that I could find to send start up parameters to a worker.

    create database dlav;

    CREATE TABLE public.asset_forrest (
        asset_id UUID NOT NULL DEFAULT gen_random_uuid(),
        asset_name STRING NOT NULL,
        asset_type STRING NOT NULL,
        asset_tree_name STRING NOT NULL,
        asset_tree_root_id UUID NOT NULL,
        asset_tree_level INT8 NOT NULL,
        version STRING NULL,
        create_time TIMESTAMP NULL DEFAULT now():::TIMESTAMP,
        last_modified_time TIMESTAMP NULL DEFAULT now():::TIMESTAMP,
        asset_status STRING NULL,
        is_parent BOOL NOT NULL,
        is_leaf BOOL NOT NULL,
        media_uri STRING NULL,
        start_ts INT8 NULL,
        end_ts INT8 NULL,
        iidx JSONB NULL,
        CONSTRAINT "primary" PRIMARY KEY (asset_tree_name ASC, asset_type ASC, asset_name ASC, asset_id ASC),
        CONSTRAINT asset_forrest_asset_type_fkey FOREIGN KEY (asset_type) REFERENCES public.asset_type_def(asset_type) ON DELETE CASCADE,
        UNIQUE INDEX asset_forrest_asset_id_key (asset_id ASC),
        INDEX idx_dlav_asset_forrest (asset_tree_name ASC, asset_tree_root_id ASC, asset_type ASC, asset_name ASC, start_ts ASC, end_ts ASC, asset_tree_level ASC, asset_status ASC, version ASC) STORING (media_uri, is_parent, is_leaf),
        INVERTED INDEX iidx_dlav_asset_forrest (iidx),
        INDEX asset_forrest_start_ts_idx (start_ts ASC) STORING (media_uri, end_ts, iidx),
        INDEX asset_forrest_asset_tree_name_asset_tree_level_idx (asset_tree_name ASC, asset_tree_level ASC) STORING (asset_tree_root_id),
        INDEX asset_forrest_asset_tree_root_id_asset_type_idx (asset_tree_root_id ASC, asset_type ASC) STORING (asset_tree_level, start_ts, end_ts, iidx),
        INDEX asset_forrest_asset_tree_root_id_asset_type_end_ts_idx (asset_tree_root_id ASC, asset_type ASC, end_ts ASC) STORING (asset_tree_level, start_ts, iidx),
        UNIQUE INDEX asset_forrest_asset_id_key1 (asset_id ASC) STORING (asset_tree_root_id, media_uri, start_ts, end_ts, iidx)
    )



    """
    def __init__(self, application_name = 'load_asset_forrst', use_aws_secret=False, secret_name=None, region_name=None, auto_commit=False, connection_dict=None, update_rec_with_leaseholder=False, use_multi_row_insert = False):
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

        sql_statement = 'insert into asset_forrest(asset_name, asset_type, asset_tree_name, asset_tree_root_id, asset_tree_level, version, is_parent, is_leaf, iidx, media_uri) values '
      
        if self.use_multi_row_insert:
            sql_statement += " %s returning asset_id"
        else:
            sql_statement += " (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) returning asset_id"

        jsonb_value = '{"first_name": "Lola", "last_name": "Dog", "location": "NYC", "online" : true, "friends" : 547}'

        print('Start the processing of the queue...')

        #         asset_name,    asset_type,  asset_tree_name, asset_tree_root_id,  asset_tree_level, version, is_parent,  is_leaf, iidx,        media_uri
        values = ['asset_name', 'TYPE1',      'asset_tree_name', 0,                 1,                1,       False,      True,    jsonb_value, self.fake.url()]
        for args in iter(input.get, 'STOP'):
            number_of_levels = random.randint(3,10)
            for level in range(number_of_levels):
                # based on anecdotal testing, I believe a cursor.execute is faster than execute_values for single row inserts
                # if there is only one arg has length one, then this is a single row insert.  
                if level == 0:
                    values[6] = True    # is_parent
                    values[7] = False   # is_leaf
                    asset_tree_root_id = '00000000-0000-0000-0000-000000000000'
                else:
                    values[6] = False    # is_parent
                    values[7] = True     # is_leaf
                values[4] = level              # asset_tree_level
                values[3] = asset_tree_root_id # asset_tree_root_id
                if (len(args)) == 1:
                    tic = time.perf_counter()
                    cursor.execute(sql_statement, values)
                    toc = time.perf_counter()
                    if level == 0:
                        asset_tree_root_id = cursor.fetchone()[0]
                        cursor.execute('update asset_forrest set asset_tree_root_id = %s where asset_id = %s', [asset_tree_root_id, asset_tree_root_id])
                else:
                    # We're going to do a multi-row insert
                    values = [(values) for arg in args]    
                    # change the last value in the list (the url) to a new url, so that all records have a unique url
                    values = [value[:-1] + [self.fake.url()] for value in values]
                    tic = time.perf_counter()
                    execute_values(cursor, sql_statement, values)
                    toc = time.perf_counter()
                    print(cursor.fetchone())
                    # result = 'from worker: {}\tthe args are: {}'.format(current_process().name, args)
                output.put({'worker':current_process().name, 'insert': toc-tic, 'records': 1})
        # print('Worker {} is done.'.format(current_process().name))


        