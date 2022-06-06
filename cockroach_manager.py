import psycopg2
import json

class CockroachManager():
    """A Note on connecting:
    if you want to use secrets to connect to the database, you to have boto3, base64 and botocore.exceptions in the python environment (pip3 install boto3, etc.)
    To use secrets, you must have set up an IAM Policy and Role for the user who's secret and secret key has been defined and you must have run aws configure to set those values.

    To connect using a dictionary, the following is an example of the data you need in the dictionary
    {
        "user": "put your database username here",
        "password": "put your database password here",
        "host": "put your hostname or ip address here",
        "port": "26257",
        "dbname": "put your dbname here",
        "sslrootcert": "put the location and name of your ca.crt here"
    }    """
    def __init__(self, connect_dict, auto_commit=False):
        """Return a connection to CockroachDB
        
        auto_commit boolean:
            do you want the connection to be autocommited?
        """

        # start by converting the incoming dictionary to a string (data source name)
        connect_dsn = ' '.join([(key + '='+ val) for (key, val) in connect_dict.items()])
        print(connect_dsn)
        try:
            self.connection = psycopg2.connect(connect_dsn)
        # try:
        #     self.connection = psycopg2.connect(
        #         user = connect_dict['user'],
        #         host = connect_dict['host'],
        #         port =  connect_dict['port'],
        #         database =  connect_dict['database'],
        #         sslmode = connect_dict['sslmode'],
        #         sslrootcert =  connect_dict['sslrootcert'],
        #         sslcert = connect_dict['sslcert'],
        #         sslkey = connect_dict['sslkey']
        #     )
            self.connection.set_session(autocommit=auto_commit)
        except (Exception, psycopg2.DatabaseError) as error:
            print("Error while connecting to PostgreSQL", error) 
            self.connection = False

    def __del__(self):
        self.connection.close()   

    @classmethod
    def use_secret(cls, auto_commit=False):
        import boto3
        import base64
        from botocore.exceptions import ClientError
        import json
        """Return a secret from AWS Secret Manager"""
        secret_name = "/nollen/nollen-cmek-cluster"
        region_name = "us-west-2"

        # Create a Secrets Manager client
        session = boto3.session.Session()
        client = session.client(
            service_name='secretsmanager',
            region_name=region_name
        )

        # In this sample we only handle the specific exceptions for the 'GetSecretValue' API.
        # See https://docs.aws.amazon.com/secretsmanager/latest/apireference/API_GetSecretValue.html
        # We rethrow the exception by default.

        try:
            get_secret_value_response = client.get_secret_value(
                # TODO
                # This too, needs to be a parameter!  Hello, argparse?
                SecretId=secret_name
            )
        except ClientError as e:
            if e.response['Error']['Code'] == 'DecryptionFailureException':
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InternalServiceErrorException':
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidParameterException':
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'InvalidRequestException':
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response['Error']['Code'] == 'ResourceNotFoundException':
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
        else:
            # Decrypts secret using the associated KMS key.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if 'SecretString' in get_secret_value_response:
                secret = get_secret_value_response['SecretString']
            else:
                decoded_binary_secret = base64.b64decode(get_secret_value_response['SecretBinary'])
        return cls(json.loads(secret), auto_commit)

