import boto3
import configparser
parser = configparser.ConfigParser()
parser.read('connection.conf')
access_key = parser.get('aws_boto3', 'access_key')
secret_key = parser.get('aws_boto3', 'secret_key')
bucket_name = parser.get('aws_boto3', 'bucket_name')
region = parser.get('aws_boto3', 'region')

sess = boto3.Session(
    aws_access_key_id=access_key,
    aws_secret_access_key=secret_key,
    region_name=region
)

s3_client = sess.client('s3')
s3_resource = sess.resource('s3')

if __name__ == '__main__':
    create_bucket = s3_client.create_bucket(
        Bucket=bucket_name,
        CreateBucketConfiguration={'LocationConstraint': 'us-west-2'}
    )

    print(f'Buycket process information-- {create_bucket}')

    client_response = s3_client.list_buckets()
    buckets_from_boto3_client = [bucket['Name'] for bucket in client_response['Buckets']]
    print(f'Bucket name given by the BOTO3.CLIENT service-- {buckets_from_boto3_client}, count is {len(buckets_from_boto3_client)} buckets')

    resource_response = s3_resource.buckets.all()
    buckets_from_boto3_resource = [bucket.name for bucket in resource_response]
    print(f'Bucket name given by the BOTO3.RESOURCE service--{buckets_from_boto3_resource}, count is {len(buckets_from_boto3_resource)} buckets')