import boto3
redshift = boto3.client('redshift')

try:
    response = redshift.delete_cluster(
        ClusterIdentifier='my-redshift-cluster',
        SkipFinalClusterSnapshot=True
    )
    print(f"Cluster {response['Cluster']['ClusterIdentifier']} is being deleted.")
except Exception as e:
    print(f"Error: {e}")
