# -*- coding: utf-8 -*-

import boto3
import json

def lambda_handler(event, context):
    print ("New S3 inventory data generated")
    
    try: 
        if (event!=None and event.has_key('Records') and
            len(event.get('Records')) == 1 and
            event.get('Records')[0].has_key('s3') and
            event.get('Records')[0].get('s3').has_key('object') and
            event.get('Records')[0].get('s3').get('object').has_key('key')):
            
            bucket_name = event.get('Records')[0].get('s3').get('bucket').get('name')
            key_name = event.get('Records')[0].get('s3').get('object').get('key')
            region = event.get('Records')[0].get('awsRegion')
            
            object_URL = 'https://s3-'+region+'.amazonaws.com/'+bucket_name+'/' + key_name
            message = 'Hi, new S3 inventory data has been received. You can download at ' + object_URL
            
            sns = boto3.client('sns')
            response = sns.publish (
                TopicArn = '<topic_arn>',
                Message = message,
                Subject = 'New S3 Inventory Data Received'
            )
            print ("SNS Message has been sent successfully!")
            s3_object = boto3.resource('s3')
            object_acl = s3_object.ObjectAcl(bucket_name,key_name)
            response = object_acl.put(ACL='public-read')
            print (response)
            print ("The ACL of the new object has changed to public-read!")
            return {'status' : 'ok'}
        else :
            return {'status' : 'ignored', 'message' : 'somethings wrong'}
    except Exception as exception:
        return {'status' : 'error', 'message' : exception.message}
    print "lambda function ended"
