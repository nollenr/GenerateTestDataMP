import mpqueue
from multiprocessing import Process, Queue, current_process
import cockroach_manager

"""
Table used in this load experiment
 create table ips(
    id  UUID DEFAULT gen_random_uuid() primary key,
    create_time TIMESTAMPTZ default now() NOT NULL,
    worker varchar(50) not null,
    cluster_node int not null,
    gateway_region varchar(50),
    gateway_az     varchar(50),
    lease_holder int,
    int8_col int8 not null,
    varchar50_col varchar(50) not null,
    bool_col bool not null,
    jsonb_col jsonb not null);

begin;
    create unique index ips_1 on ips(create_time, cluster_node, int8_col) where int8_col is not null;
    create index ips_2 on ips(create_time);
    create index ips_3 on ips(int8_col);
end;
"""

if __name__ == '__main__':

    NUMBER_OF_WORKERS=30
    NUMBER_OF_TASKS=100000
    INCLUDE_LEASEHOLDER = False
    USE_UNIQUE_INDEX = False
    UNIQUE_INDEX_VALUE_OFFSET=0
    # Database connection details will either be from an AWS Secret, or they'll have
    # to be supplied as a connection dictionary
    GET_DATABASE_CONNECTION_DETAILS_FROM_AWS_SECRET = True

    """
    if I'm going to have a unique index on the table, then I need to make the 
    column that is indexed, be unique.  To do that, I'm going to need to get 
    the max value of that column, prior to starting workers
    """

    # The Cockroach Manager Class allows connection via secret manager or a connection string.
    if GET_DATABASE_CONNECTION_DETAILS_FROM_AWS_SECRET:
        connect_dict = None
    else:
            connect_dict = {
            "username": "ron",
            "password": "adfadfadsfsafdsa",
            "host": "nollen-klei-demo-nlb-c9eee36bd5301663.elb.us-west-2.amazonaws.com",
            "port": "26257",
            "dbname": "defaultdb",
            "ca.crt": "/home/ec2-user/Library/CockroachCloud/certs/nollen-klei-demo-ca.crt"
        }
    
    # Initialize the multiprocesssing class so that the worker can be started and passed execution parameters.
    mpunit = mpqueue.MPQueue(application_name = 'IPS', use_aws_secret = GET_DATABASE_CONNECTION_DETAILS_FROM_AWS_SECRET, connection_dict=connect_dict, update_rec_with_leaseholder=INCLUDE_LEASEHOLDER, unique_index_value_offset = UNIQUE_INDEX_VALUE_OFFSET)

    for i in range(NUMBER_OF_WORKERS):
        Process(target=mpunit.worker, args=(mpunit.task_queue, mpunit.done_queue)).start()

    for i in range(NUMBER_OF_TASKS):
        mpunit.task_queue.put((i,i))
        # if mpunit.task_queue.qsize() > 10000:
        # #     print('task queue size is over 1000 records.')
        #     time.sleep(0.1)
    print('done putting stuff on queue.  Number of items on task queue is {}'.format(mpunit.task_queue.qsize()))

    for i in range(NUMBER_OF_TASKS):
        mpunit.done_queue.get()
        # print('\t', done_queue.get())
    print('done getting stuff from queue.  Number of items on done queue is {}'.format(mpunit.done_queue.qsize()))

    for i in range(NUMBER_OF_WORKERS):
        mpunit.task_queue.put('STOP')