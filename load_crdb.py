from pickle import FALSE
import mpqueue
from multiprocessing import Process, Queue, current_process

"""
Table used in this load experiment
create table ips(
    id              UUID            DEFAULT gen_random_uuid()   primary key,
    create_time     TIMESTAMPTZ     default now()               NOT NULL,
    rowid           INT8            DEFAULT unique_rowid()      NOT NULL,
    worker          varchar(50)                                 not null,
    cluster_node    int                                         not null,
    gateway_region  varchar(50),
    gateway_az      varchar(50),
    lease_holder    int,
    int8_col        int8,
    varchar50_col   varchar(50)                                 not null,
    bool_col        bool                                        not null,
    jsonb_col       jsonb                                       not null
);

begin;
    create unique index ips_1 on ips(rowid) where int8_col is not null;
    create index ips_2 on ips(create_time);
    create index ips_3 on ips(int8_col);
end;
"""

if __name__ == '__main__':

    NUMBER_OF_WORKERS=1
    NUMBER_OF_TASKS=1
    INCLUDE_LEASEHOLDER = False
    USE_UNIQUE_INDEX = False
    USE_MULTI_ROW_INSERT = False
    MUTLI_ROW_INSERT_SIZE = 100
    AUTO_COMMIT = True
    # Database connection details will either be from an AWS Secret, or they'll have
    # to be supplied as a connection dictionary
    GET_DATABASE_CONNECTION_DETAILS_FROM_AWS_SECRET = False
    SECRET_NAME="/nollen/nollen-cmek-cluster"
    REGION_NAME="us-west-2"

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
            "user"          : "myuser",
            "host"          : "example.host.com",
            "port"          : "26257",
            "dbname"        : "defaultdb",
            "sslmode"       : "require",
            "sslrootcert"   : "/home/ec2-user/Library/CockroachCloud/certs/ca.crt",
            "sslcert"       : "/home/ec2-user/Library/CockroachCloud/certs/myuser.crt",
            "sslkey"        : "/home/ec2-user/Library/CockroachCloud/certs/myuser.key"
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
            "host": "internal-nollen-cmek-cluster-7jd.aws-us-west-2.cockroachlabs.cloud",
            "port": "26257",
            "dbname": "defaultdb",
            "sslrootcert": "/home/ec2-user/Library/CockroachCloud/certs/nollen-cmek-cluster-ca.crt"
            }
    # Initialize the multiprocesssing class so that the worker can be started and passed execution parameters.
    mpunit = mpqueue.MPQueue(application_name = 'IPS', use_aws_secret = GET_DATABASE_CONNECTION_DETAILS_FROM_AWS_SECRET, secret_name=SECRET_NAME, region_name=REGION_NAME, auto_commit=AUTO_COMMIT, connection_dict=connect_dict, update_rec_with_leaseholder=INCLUDE_LEASEHOLDER, use_multi_row_insert=USE_MULTI_ROW_INSERT)

    for i in range(NUMBER_OF_WORKERS):
        Process(target=mpunit.worker, args=(mpunit.task_queue, mpunit.done_queue)).start()

    for i in range(NUMBER_OF_TASKS):
        data = ((i,),)
        if USE_MULTI_ROW_INSERT:
            data = ((i,),) * MUTLI_ROW_INSERT_SIZE
        mpunit.task_queue.put(data)
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