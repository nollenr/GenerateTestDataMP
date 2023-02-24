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

    For start_ts and end_ts: 
    1/1/2023 0:0:01 (1 second past midnight): 1672531201
    12/31/2023 23:23:59:  1704065039

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
        self.is_parent = False
        self.is_leaf = True
        self.child_range_size = 1000
        self.subchild_range_size = 100

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

        sql_statement_i_asset = 'select * from asset_forrest where asset_id = %s'
        sql_statement_closure = 'select asset_tree_name,asset_tree_root_id,ancestor_asset_id,ancestor_asset_type,ancestor_asset_name,level_distance,descendent_asset_id,descendent_asset_type,descendent_asset_name,is_parent,is_leaf from asset_forrest_closure where descendent_asset_id = %s'
        insert_statement_forrest = 'insert into asset_forrest(asset_id, asset_name, asset_type, asset_tree_name, asset_tree_root_id, asset_tree_level, is_parent, is_leaf, media_uri, start_ts, end_ts, iidx) values %s'
        insert_multi_closure = 'insert into asset_forrest_closure (asset_tree_name,asset_tree_root_id,ancestor_asset_id,ancestor_asset_type,ancestor_asset_name,level_distance,descendent_asset_id,descendent_asset_type,descendent_asset_name,is_parent,is_leaf) values %s'
        jsonb_value = '{"first_name": "Ron", "last_name": "Dog", "location": "NYC", "online" : true, "friends" : 1234567890}'

        print('Start the processing of the queue...')

        for args in iter(input.get, 'STOP'):

            tic = time.perf_counter()
            # Get the I record.  Only need to do this once.  This will be the basis for some of the new asset fields.  For example they'll have the same tree root and name
            cursor.execute(sql_statement_i_asset, args[0])
            i_record = cursor.fetchone()
            # print('i_record: {}'.format(i_record))

            # Get the closure records for the existing 'I' record.  For each of the 1 million child records, this set of records will be duplicated, plus one additional for the new child.
            cursor.execute(sql_statement_closure, (args[0]))
            closure_records = cursor.fetchall()
            # print(closure_records)
            toc = time.perf_counter()
            print('worker: {} completed opening queries in {}.  Starting loops.'.format(current_process().name, toc-tic))



            tic = time.perf_counter()
            for child in range(self.child_range_size):
                forrest_values = []
                closure_values = []
                for subchild in range(self.subchild_range_size):
                    # 1 million loop here
                    # Create all of the UUIDs needed for the entire tree we're inserting
                    asset_id   = str(uuid.uuid4())
                    asset_name = str(uuid.uuid4())
                    media_url  = self.fake.url()


                    # This block inserts the "new child record in to the forrest table -- this is needed before the closure records can be created"
                    #                (asset_id, asset_name,  asset_type, asset_tree_name, asset_tree_root_id, asset_tree_level, is_parent,      is_leaf,      media_uri, start_ts,   end_ts,     iidx)
                    forrest_values.append([asset_id, asset_name, 'TYPE1',    i_record[3],      i_record[4],        3,                self.is_parent, self.is_leaf, media_url, 1672531201, 1704065039, jsonb_value]);

                    for cr in closure_records: # this should be 2 records ("A" and "C")
                    #                    asset_tree_name,asset_tree_root_id,ancestor_asset_id,ancestor_asset_type,ancestor_asset_name,level_distance+1,'a91b06b5-cbc2-4bb5-b57d-c2f6b52c03cb',descendent_asset_type,'23cdf6cf-f78d-463a-bec4-ba913185951c',is_parent,is_leaf 
                    #                    asset_tree_name,asset_tree_root_id,ancestor_asset_id,ancestor_asset_type,ancestor_asset_name,level_distance,descendent_asset_id,descendent_asset_type,descendent_asset_name,is_parent,is_leaf
                        closure_values.append([cr[0],          cr[1],             cr[2],            cr[3],              cr[4],              cr[5]+1,        asset_id,          cr[7],                asset_name,            cr[9],   cr[10]])

                    # this is the closure record for the new record back to the "I" record.  The above links from the "A" record and "C" Record
                    #                asset_tree_name,asset_tree_root_id,ancestor_asset_id,ancestor_asset_type,ancestor_asset_name,level_distance,descendent_asset_id,descendent_asset_type,descendent_asset_name,is_parent,      is_leaf
                    closure_values.append([i_record[3],    i_record[4],       i_record[0],      i_record[2],        i_record[2],        1,             asset_id,           i_record[2],          asset_name,           self.is_parent, self.is_leaf])
                #100 loop ends here
                execute_values(cursor, insert_statement_forrest, forrest_values)
                execute_values(cursor, insert_multi_closure, closure_values)
            
            toc = time.perf_counter()
            print('worker: {} processed {} forest records and {} closure records in {}'.format(current_process().name, self.child_range_size* self.subchild_range_size, self.child_range_size * self.subchild_range_size * 3, toc-tic))
            output.put({'worker':current_process().name, 'i_record': args[0], 'records': 1})
        # print('Worker {} is done.'.format(current_process().name))


        