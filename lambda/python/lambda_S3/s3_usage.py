# Lambda function for gathering each bucket's BucketSizeBytes / NumberOfObjects

import boto3
import json
from datetime import datetime
from datetime import timedelta

def lambda_handler(event, context):
    
    # Get bucket list and store it 
    s3_handle = boto3.client('s3')
    bucket_list = s3_handle.list_buckets().get('Buckets')
    
    # Get current date
    today = datetime.now()
    
    # Message Definition
    header = "Bucket Name\tDate\tSize(Standard)\tSize(IA)\tNumberOfObjects\n"
    message = "Current S3 usage per each bucket:\n\n"+header
    
    # iterate each bucket in account
    for bucket_raw in bucket_list:
        
        bucket = bucket_raw.get('Name')
        cloudwatch_handle = boto3.client('cloudwatch')
        standard = cloudwatch_handle.get_metric_statistics (
            Namespace = 'AWS/S3',
            MetricName = 'BucketSizeBytes',
            StartTime = today - timedelta(days=+1),
            EndTime = today,
            Unit = 'Bytes',
            Period = 86400,
            Dimensions = [
                {
                    'Name' : 'StorageType',
                    'Value' : 'StandardStorage'
                },
                {
                    'Name' : 'BucketName',
                    'Value' : bucket
                }
            ],
            Statistics = ['Average']
        ).get('Datapoints')
        standard_ia = cloudwatch_handle.get_metric_statistics (
            Namespace = 'AWS/S3',
            MetricName = 'BucketSizeBytes',
            StartTime = today - timedelta(days=+1),
            EndTime = today,
            Unit = 'Bytes',
            Period = 86400,
            Dimensions = [
                {
                    'Name' : 'StorageType',
                    'Value' : 'StandardIAStorage'
                },
                {
                    'Name' : 'BucketName',
                    'Value' : bucket
                }
            ],
            Statistics = ['Average']
        ).get('Datapoints')
        objectnum = cloudwatch_handle.get_metric_statistics (
            Namespace = 'AWS/S3',
            MetricName = 'NumberOfObjects',
            StartTime = today - timedelta(days=+1),
            EndTime = today,
            Unit = 'Count',
            Period = 86400,
            Dimensions = [
                {
                    'Name' : 'StorageType',
                    'Value' : 'AllStorageTypes'
                },
                {
                    'Name' : 'BucketName',
                    'Value' : bucket
                }
            ],
            Statistics = ['Average']
        ).get('Datapoints')
        
        # Output manipulation
        
        if len(standard):
            metric_date = standard[0].get('Timestamp').date()
        elif len(standard_ia):
            metric_date = standard_ia[0].get('Timestamp').date()
        elif len(objectnum):
            metric_date = objectnum[0].get('Average')
        else:
            metric_date = datetime(1900,1,1).date()
        if len(standard):
            bytes_standard = standard[0].get('Average')
        else:
            bytes_standard = 0
        if len(standard_ia):
            bytes_standard_ia = standard_ia[0].get('Average')
        else:
            bytes_standard_ia = 0
        if len(objectnum):
            num_object = objectnum[0].get('Average')
        else:
            num_object = 0
        
        # generation output line
        
        message = message + bucket +"\t" + metric_date.isoformat() +"\t"
        message = message + str(bytes_standard) +"\t" + str(bytes_standard_ia) +"\t"
        message = message + str(num_object) + "\n"
        
    sns = boto3.client('sns')
    response = sns.publish (
        TopicArn = '<topic arn>',
        Message = message,
        Subject = 'S3 Usage Report'
    )
        
        
        
        

    
    
    
    
