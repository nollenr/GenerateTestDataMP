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

    x=[
        [0, "A-A-A", "A-A-A", "ANAME",  []],
        [1, "B-B-B", "A-A-A", "BNAME",  [[0, "A-A-A", "ANAME"]]],
        [1, "C-C-C", "A-A-A", "CNAME",  [[0, "A-A-A", "ANAME"]]],
        [2, "D-D-D", "A-A-A", "DNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"]]],
        [2, "E-E-E", "A-A-A", "ENAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"]]],
        [2, "F-F-F", "A-A-A", "FNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"]]],
        [2, "G-G-G", "A-A-A", "GNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"]]],
        [2, "H-H-H", "A-A-A", "HNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"]]],
        [2, "I-I-I", "A-A-A", "INAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"]]],
        [3, "J-J-J", "A-A-A", "JNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "D-D-D", "DNAME"]]],
        [3, "K-K-K", "A-A-A", "KNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "D-D-D", "DNAME"]]],
        [3, "L-L-L", "A-A-A", "LNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "D-D-D", "DNAME"]]],
        [3, "M-M-M", "A-A-A", "MNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "D-D-D", "DNAME"]]],
        [3, "N-N-N", "A-A-A", "NNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "E-E-E", "ENAME"]]],
        [3, "O-O-O", "A-A-A", "ONAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "E-E-E", "ENAME"]]],
        [3, "P-P-P", "A-A-A", "PNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "F-F-F", "FNAME"]]],
        [3, "Q-Q-Q", "A-A-A", "QNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "F-F-F", "FNAME"]]],
        [3, "R-R-R", "A-A-A", "RNAME",  [[0, "A-A-A", "ANAME"], [1, "B-B-B", "BNAME"], [2, "F-F-F", "FNAME"]]],
        [3, "S-S-S", "A-A-A", "SNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"]]],
        [3, "T-T-T", "A-A-A", "TNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"]]],
        [3, "U-U-U", "A-A-A", "UNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"]]],
        [3, "V-V-V", "A-A-A", "VNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"]]],
        [3, "W-W-W", "A-A-A", "WNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "H-H-H", "HNAME"]]],
        [3, "X-X-X", "A-A-A", "XNAME",  [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "H-H-H", "HNAME"]]],
        [4, "AA",    "A-A-A", "AANAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"]]]    
        [4, "BB",    "A-A-A", "BBNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"]]]    
        [4, "CC",    "A-A-A", "CCNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"]]]    
        [4, "DD",    "A-A-A", "DDNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "H-H-H", "HNAME"], [3, "W-W-W", "WNAME"]]]    
        [4, "EE",    "A-A-A", "EENAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "H-H-H", "HNAME"], [3, "W-W-W", "WNAME"]]]    
        [4, "FF",    "A-A-A", "FFNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "H-H-H", "HNAME"], [3, "W-W-W", "WNAME"]]]    
        [5, "GG",    "A-A-A", "GGNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"], [4, "CC", "CNAME"]]]    
        [5, "HH",    "A-A-A", "HHNAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"], [4, "CC", "CNAME"]]]    
        [5, "II",    "A-A-A", "IINAME", [[0, "A-A-A", "ANAME"], [1, "C-C-C", "CNAME"], [2, "G-G-G", "GNAME"], [3, "S-S-S", "SNAME"], [4, "CC", "CNAME"]]]    
    ]

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

        sql_statement_forrest = 'insert into asset_forrest(asset_id, asset_name, asset_type, asset_tree_name, asset_tree_root_id, asset_tree_level, version, is_parent, is_leaf, iidx, media_uri, start_ts, end_ts) values '
        sql_statement_forrest += " %s"

        sql_statement_closure = 'insert into asset_forrest_closure(asset_tree_name,asset_tree_root_id,ancestor_asset_id,ancestor_asset_type,ancestor_asset_name,level_distance,descendent_asset_id,descendent_asset_type,descendent_asset_name,is_parent,is_leaf) values'
        sql_statement_closure += " %s"

        jsonb_value = '{"first_name": "Lola", "last_name": "Dog", "location": "NYC", "online" : true, "friends" : 547}'

        print('Start the processing of the queue...')

        for args in iter(input.get, 'STOP'):

            # Create all of the UUIDs needed for the entire tree we're inserting
            A=str(uuid.uuid4())
            B=str(uuid.uuid4())
            C=str(uuid.uuid4())
            D=str(uuid.uuid4())
            E=str(uuid.uuid4())
            F=str(uuid.uuid4())
            G=str(uuid.uuid4())
            H=str(uuid.uuid4())
            I=str(uuid.uuid4())
            J=str(uuid.uuid4())
            K=str(uuid.uuid4())
            L=str(uuid.uuid4())
            M=str(uuid.uuid4())
            N=str(uuid.uuid4())
            O=str(uuid.uuid4())
            P=str(uuid.uuid4())
            Q=str(uuid.uuid4())
            R=str(uuid.uuid4())
            S=str(uuid.uuid4())
            T=str(uuid.uuid4())
            U=str(uuid.uuid4())
            V=str(uuid.uuid4())
            W=str(uuid.uuid4())
            X=str(uuid.uuid4())
            AA=str(uuid.uuid4())
            BB=str(uuid.uuid4())
            CC=str(uuid.uuid4())
            DD=str(uuid.uuid4())
            EE=str(uuid.uuid4())
            FF=str(uuid.uuid4())
            GG=str(uuid.uuid4())
            HH=str(uuid.uuid4())
            II=str(uuid.uuid4())

            # Although these are names, to keep them simple and unique I am using uuids in this example.
            ANAME=str(uuid.uuid4())
            BNAME=str(uuid.uuid4())
            CNAME=str(uuid.uuid4())
            DNAME=str(uuid.uuid4())
            ENAME=str(uuid.uuid4())
            FNAME=str(uuid.uuid4())
            GNAME=str(uuid.uuid4())
            HNAME=str(uuid.uuid4())
            INAME=str(uuid.uuid4())
            JNAME=str(uuid.uuid4())
            KNAME=str(uuid.uuid4())
            LNAME=str(uuid.uuid4())
            MNAME=str(uuid.uuid4())
            NNAME=str(uuid.uuid4())
            ONAME=str(uuid.uuid4())
            PNAME=str(uuid.uuid4())
            QNAME=str(uuid.uuid4())
            RNAME=str(uuid.uuid4())
            SNAME=str(uuid.uuid4())
            TNAME=str(uuid.uuid4())
            UNAME=str(uuid.uuid4())
            VNAME=str(uuid.uuid4())
            WNAME=str(uuid.uuid4())
            XNAME=str(uuid.uuid4())
            AANAME=str(uuid.uuid4())
            BBNAME=str(uuid.uuid4())
            CCNAME=str(uuid.uuid4())
            DDNAME=str(uuid.uuid4())
            EENAME=str(uuid.uuid4())
            FFNAME=str(uuid.uuid4())
            GGNAME=str(uuid.uuid4())
            HHNAME=str(uuid.uuid4())
            IINAME=str(uuid.uuid4())

            ASSET_TREE_NAME=str(uuid.uuid4())

            # build a string that represents the tree
            x='[[0, "'+A+'", "'+A+'", "'+ANAME+'",  []],[1, "'+B+'", "'+A+'", "'+BNAME+'",  [[0, "'+A+'", "'+ANAME+'"]]],[1, "'+C+'", "'+A+'", "'+CNAME+'",  [[0, "'+A+'", "'+ANAME+'"]]],[2, "'+D+'", "'+A+'", "'+DNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"]]],[2, "'+E+'", "'+A+'", "'+ENAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"]]],[2, "'+F+'", "'+A+'", "'+FNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"]]],[2, "'+G+'", "'+A+'", "'+GNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"]]],[2, "'+H+'", "'+A+'", "'+HNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"]]],[2, "'+I+'", "'+A+'", "'+INAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"]]],[3, "'+J+'", "'+A+'", "'+JNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+D+'", "'+DNAME+'"]]],[3, "'+K+'", "'+A+'", "'+KNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+D+'", "'+DNAME+'"]]],[3, "'+L+'", "'+A+'", "'+LNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+D+'", "'+DNAME+'"]]],[3, "'+M+'", "'+A+'", "'+MNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+D+'", "'+DNAME+'"]]],[3, "'+N+'", "'+A+'", "'+NNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+E+'", "'+ENAME+'"]]],[3, "'+O+'", "'+A+'", "'+ONAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+E+'", "'+ENAME+'"]]],[3, "'+P+'", "'+A+'", "'+PNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+F+'", "'+FNAME+'"]]],[3, "'+Q+'", "'+A+'", "'+QNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+F+'", "'+FNAME+'"]]],[3, "'+R+'", "'+A+'", "'+RNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+B+'", "'+BNAME+'"], [2, "'+F+'", "'+FNAME+'"]]],[3, "'+S+'", "'+A+'", "'+SNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"]]],[3, "'+T+'", "'+A+'", "'+TNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"]]],[3, "'+U+'", "'+A+'", "'+UNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"]]],[3, "'+V+'", "'+A+'", "'+VNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"]]],[3, "'+W+'", "'+A+'", "'+WNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+H+'", "'+HNAME+'"]]],[3, "'+X+'", "'+A+'", "'+XNAME+'",  [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+H+'", "'+HNAME+'"]]],[4, "'+AA+'","'+A+'", "'+AANAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"]]],    [4, "'+BB+'","'+A+'", "'+BBNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"]]],    [4, "'+CC+'","'+A+'", "'+CCNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"]]],    [4, "'+DD+'","'+A+'", "'+DDNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+H+'", "'+HNAME+'"], [3, "'+W+'", "'+WNAME+'"]]],    [4, "'+EE+'","'+A+'", "'+EENAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+H+'", "'+HNAME+'"], [3, "'+W+'", "'+WNAME+'"]]],    [4, "'+FF+'","'+A+'", "'+FFNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+H+'", "'+HNAME+'"], [3, "'+W+'", "'+WNAME+'"]]],    [5, "'+GG+'","'+A+'", "'+GGNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"], [4, "'+CC+'", "'+CNAME+'"]]],    [5, "'+HH+'","'+A+'", "'+HHNAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"], [4, "'+CC+'", "'+CNAME+'"]]],    [5, "'+II+'","'+A+'", "'+IINAME+'", [[0, "'+A+'", "'+ANAME+'"], [1, "'+C+'", "'+CNAME+'"], [2, "'+G+'", "'+GNAME+'"], [3, "'+S+'", "'+SNAME+'"], [4, "'+CC+'", "'+CNAME+'"]]]]'
            y=json.loads(x)

            multi_value_forrest_list=[]
            multi_value_closure_list=[]
            for branch in y:
                if branch[0] == 0:
                    IS_PARENT = True
                    IS_LEAF = False
                else:
                    IS_PARENT = False
                    IS_LEAF = True
                #                                asset_id, asset_name, asset_type, asset_tree_name, asset_tree_root_id,  asset_tree_level, version, is_parent,  is_leaf, iidx,        media_uri,       start_ts,   end_ts      
                multi_value_forrest_list.append([branch[1],branch[3], 'TYPE1',     ASSET_TREE_NAME, branch[2],           branch[0],        0      , IS_PARENT,  IS_LEAF, jsonb_value, self.fake.url(), 1672531201, 1704065039])

                for closure in branch[4]:
                    #                                asset_tree_name,asset_tree_root_id,ancestor_asset_id,ancestor_asset_type,ancestor_asset_name,level_distance,       descendent_asset_id,descendent_asset_type,descendent_asset_name,is_parent,is_leaf)'
                    multi_value_closure_list.append([ASSET_TREE_NAME,A,                  closure[1],      'TYPE1',            closure[2],         branch[0]-closure[0], branch[1],          'TYPE1',              branch[3],            False,    True])

            # We're going to do a multi-row insert
            tic = time.perf_counter()

            try:
                execute_values(cursor, sql_statement_forrest, multi_value_forrest_list)
                execute_values(cursor, sql_statement_closure, multi_value_closure_list)
            except:
                time.sleep(300)
                crdb = cockroach_manager.CockroachManager(self.connection_dict, self.auto_commit)
                cursor = crdb.connection.cursor()
                execute_values(cursor, sql_statement_forrest, multi_value_forrest_list)
                execute_values(cursor, sql_statement_closure, multi_value_closure_list)

            toc = time.perf_counter()
            # print(cursor.fetchone())
            # result = 'from worker: {}\tthe args are: {}'.format(current_process().name, args)
            output.put({'worker':current_process().name, 'insert': toc-tic, 'records': 1})
        # print('Worker {} is done.'.format(current_process().name))


        