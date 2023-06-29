import json
import boto3
import os
import urllib.parse
from post import notify_parnter

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# import urllib3
# from move import copy_file
# from proc import get_job_results
# import uuid
# from decimal import Decimal
# urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
# logger = logging.getLogger(__name__)
# s3_client = boto3.client('s3')
# http = urllib3.PoolManager(cert_reqs = 'CERT_NONE')


s3 = boto3.client('s3')
textract = boto3.client('textract')
dynamodb = boto3.resource('dynamodb')
db = dynamodb.Table('paperview_jobs')

def lambda_handler(event, context):
    # TODO implement

    event_type = 'unknown'
    try:
        if event['Records']:
            event_types = {'aws:sns':'SNS', 'aws:s3': 'S3', 'aws:sqs': 'SQS', 'aws:codecommit': 'CodeCommit',
                'aws:ses': 'SES', 'aws:dynamodb': 'DynamoDB', 'aws:kinesis': 'Kinesis'}
            
            
            event_source = event['Records'][0].get('eventSource', None)
            if not event_source:
                event_source = event['Records'][0].get('EventSource', None)
            event_type = event_types.get(event_source, 'unknown')
    except:
        logger.info('event not found')


    logger.info(event)
    logger.info(event['Records'])
    logger.info(context)
    logger.info(event_source)
    logger.info(event_type)
    
    # return initiate_textract_job(event, context)
    
    # logger.info()
    
    if event_type == 'S3':
        return initiate_textract_job(event, context)
    elif event_type == 'SNS':
        logger.info('textract update')
        return textract_status_update(event, context)
        
    return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Triger {event_type} not recognized'})
        }
 
def textract_status_update(event, context):
    logger.info('Caught SNS TextRact Status')
    
    logger.info(event)
    
    event_message = json.loads(json.dumps(event))['Records'][0]['Sns']['Message']
    print(event_message)
    print(type(event_message))
    # print(json.loads(event_message))
 
    job_status = json.loads(event_message)['Status']
    job_tag = json.loads(event_message)['JobTag']
    job_id = json.loads(event_message)['JobId']
    document_location = json.loads(event_message)['DocumentLocation']
    
    # job_status = event_message['Status']
    # job_tag = event_message['JobTag']
    # job_id = event_message['JobId']
    # document_location = event_message['DocumentLocation']
    
    S3ObjectName = json.loads(json.dumps(document_location))['S3ObjectName']
    S3Bucket = json.loads(json.dumps(document_location))['S3Bucket']
    
    logger.info(S3Bucket)
    logger.info(S3ObjectName)
    
    
    # S3ObjectName = document_location['S3ObjectName']
    # S3Bucket = document_location['S3Bucket']
    
    logger.info(f"JobTag: {job_tag}, Job Status: {job_status}")

    if(job_status == 'SUCCEEDED'):
        textract_result = get_textract_results(job_id)
        # logger.info(json.loads(textract_result))
        # WRITE TEXTRACT RESULT TO S3
        paperview_job_id = S3ObjectName.split('/')[-1].split('.')[0]
        documentscan = {'object_key': paperview_job_id, 'scan_data': textract_result}
        # data_extract_encoded = json.dumps(documentscan)
        s3_path = f'results/{paperview_job_id}.json'
     
     
        s3 = boto3.client('s3')
        json_object = s3
        s3.put_object(
             Body=json.dumps(documentscan),
             Bucket=S3Bucket,
             Key= s3_path
        )
     
        # s3Object = s3.Object(S3Bucket, s3_path)
        # s3object.put(
        #     Body=(bytes(json.dumps(documentscan).encode('UTF-8')))
        # )
        
        # UPDATE DYNAMODB
        logger.info(f'paperview_job_id: {paperview_job_id}')
        # logger.info(documentscan)
        # textract_result = json.dumps({'document': 'something made up'})
        update_dynamo_record(paperview_job_id, {'status': 'TEXTRACT_SUCCEEDED', 'textract_result': s3_path})
        
        # NOTIFY REQUESTING PARTNER
        notify_parnter(paperview_job_id)
        
        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Textract job succeeded'})
        }
        
    return {
        'statusCode': 200,
        'body': json.dumps({'message': f'Textract job failed'})
    }

def get_textract_results(job_id):

    pages = []
    response = textract.get_document_analysis(JobId=job_id)
    pages.append(response)

    nextToken = None
    if('NextToken' in response):
        nextToken = response['NextToken']

    while(nextToken):

        response = textract.get_document_analysis(JobId=job_id, NextToken=nextToken)

        pages.append(response)
        nextToken = None
        if('NextToken' in response):
            nextToken = response['NextToken']

    return pages

def update_dynamo_record(paperview_job_id, dct):
    job = db.get_item(Key={'id': paperview_job_id})
    if 'Item' in job:
        for k, v in dct.items():
            job['Item'][k] = v
        # job['Item']['status'] = 'Textract Requested'
        # job['Item']['textract_job_id'] = textract_job_id
    
        job = db.put_item(
            Item=job['Item']
        )



def initiate_textract_job(event, context):
    bucket = event['Records'][0]['s3']['bucket']['name']
    # paperview_job_id = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8').split('.')[0]
    filename = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8').split('/')[-1]
    paperview_job_id = filename.split('.')[0]

    logger.info(bucket)
    logger.info(filename)
    logger.info(paperview_job_id)

    textract_code, textract_job_id = textract_file(bucket, filename)
    logger.info(textract_code)
    logger.info(textract_job_id)
    
    if textract_job_id and textract_code==200:
        update_dynamo_record(paperview_job_id, 
                {'status': 'Textract Requested', 'textract_job_id': textract_job_id})

        # job = db.get_item(Key={'id': paperview_job_id})
        # if 'Item' in job:
        #         job['Item']['status'] = 'Textract Requested'
        #         job['Item']['textract_job_id'] = textract_job_id
    
        #         job = db.put_item(
        #             Item=job['Item']
        #         )
    
        return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Textract Job #{textract_job_id} Requested for {filename}'})
        }
    return {
            'statusCode': 200,
            'body': json.dumps({'message': f'Textract job create failed'})
        }


def textract_file(bucket, filename):
    # try:
    logger.debug(f"Extracting text from pdf {filename} in {bucket}")
    textract_code = 500
    
    response = textract.start_document_analysis(
        DocumentLocation={
            'S3Object': {
                'Bucket': bucket,
                'Name': f'uploads/{filename}',
            }
        },
        FeatureTypes=['TABLES', 'FORMS'],
        JobTag='pdf_documents',
        NotificationChannel={
            'SNSTopicArn': 'arn:aws:sns:us-west-1:159230745197:ppv_textract_job_status',
            'RoleArn': 'arn:aws:iam::159230745197:role/ppv_sns_full_access_role'
            # 'SNSTopicArn': os.environ.get('SNS_TOPIC_ARN'),
            # 'RoleArn': os.environ.get('ROLE_ARN')
        },
    )

    # return 200, 123098

    jobid = response.get('JobId', None)
    logger.debug(f"JobID: {jobid}")
    
    status_code = 500
    meta_data = response.get("ResponseMetadata", None)
    if meta_data:
        status_code = meta_data.get("HTTPStatusCode")
        
    return status_code, jobid
        
    # except Exception as e:
    #     logger.error(f"Textract PDF Error: {e}")
    #     logger.info(f"Textract PDF error while extracting {key} from {bucket}")
    #     raise e