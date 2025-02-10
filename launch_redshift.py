import boto3
import configparser
import os
parser = configparser.ConfigParser()
parser.read('connection.conf')

redshift = boto3.client("redshift", region_name="us-west-2")

cluster_identifier = "my-redshift-cluster"
db_name = "dev"
master_username = "admin"
master_password = "Password123"
node_type = "ra3.large"
number_of_nodes = 1

try:
    response = redshift.create_cluster(
        ClusterIdentifier=cluster_identifier,
        DBName=db_name,
        MasterUsername=master_username,
        MasterUserPassword=master_password,
        NodeType=node_type,
        NumberOfNodes=number_of_nodes,
        ClusterType="single-node",
        PubliclyAccessible=True,
        IamRoles=[parser.get('aws_boto3', 'iam_role')]
    )
    print("Creating Redshift cluster...")
except Exception as e:
    print(f"Error: {e}")
