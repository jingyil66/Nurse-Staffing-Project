from create_bucket import s3_resource
from create_bucket import bucket_name

bucket = s3_resource.Bucket(bucket_name)
for contents in bucket.objects.all():
    print(f'Bucket objects BEFORE DELETING-- {contents.key}')

resource_response = s3_resource.buckets.all()
buckets_from_boto3_resource = [bucket.name for bucket in resource_response]
print(f'Bucket names given by the boto3.resource service BEFORE DELETING--{buckets_from_boto3_resource}, count is {len(buckets_from_boto3_resource)} buckets')

def cleanup_s3_bucket():
    for content in bucket.objects.all():
        content.delete()
    for content in bucket.object_versions.all():
        content.delete()

cleanup_s3_bucket()
bucket.delete()

resource_response = s3_resource.buckets.all()
buckets_from_boto3_resource = [bucket.name for bucket in resource_response]
print(f'Bucket names given by the boto3.resource service AFTER DELETING--{buckets_from_boto3_resource}, count is {len(buckets_from_boto3_resource)} buckets')