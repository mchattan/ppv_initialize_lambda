import os
import json
import urllib3
import boto3

import logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
db = dynamodb.Table('paperview_jobs')
partnerDB = dynamodb.Table('paperview_partners')

# host_url = os.environ.get('HOST_URL')
# rest_api_token = os.environ.get('REST_API_TOKEN')

# headers = {
#             "Authorization": rest_api_token,
#             "user-agent": "bbg-aws-lambda/0.0.1",
#             "Content-Type": "application/json",
#             "Connection": "keep-alive",
#             "Accept": "*/*",
#             "Accept-Encoding": "gzip, deflate, br",
#         }
        
        
http = urllib3.PoolManager(cert_reqs = 'CERT_NONE')


def notify_parnter(paperview_job_id):
    logger.info(f'notify_parnter for job {paperview_job_id}')
    # return {'message': 'parnter status failed'}
    
    job = db.get_item(Key={'id': paperview_job_id})
    if 'Item' in job:
        job_status = job['Item']['status']
        partner_id = job['Item']['requesting_partner']
        json_url = job['Item']['textract_result']
    
    logger.info(partner_id)
    
    partner = partnerDB.get_item(Key={'id': partner_id})
    if 'Item' in partner:
        partner_api = partner['Item']['rest_api']
        partner_secret = partner['Item']['secret']
        partner_status = partner['Item']['status']
    else:
        logger.info('partner status failed')    
        return {'message': 'parnter not found'}
        
        
    logger.info(partner_status)

    if partner_status == 'Active':
        try:
     
            data = {
                'job_status': job_status,
                'paperview_job_id': paperview_job_id,
                'json_url': json_url
            }
            

            enc_data = json.dumps(data).encode('utf-8')
            
            # documentscan = {}
            # documentscan['object_key'] = paperview_job_id
            # documentscan['scan_data'] = data
            # data_extract_encoded = json.dumps(documentscan)
            
            logger.info(data)            
            headers = {
                "Authorization": f'Token {partner_secret}',
                "user-agent": "bbg-aws-lambda/0.0.1",
                "Content-Type": "application/json",
                "Connection": "keep-alive",
                "Accept": "*/*",
                "Accept-Encoding": "gzip, deflate, br",
            }
            response = http.request('POST',partner_api, body=enc_data, headers=headers)
            # response = request.post(partner_api, headers=headers, json=data)
            
            logger.info(response)
            return response
            
        except Exception as e:
            logger.error(f"Post Error: {e}")
            logger.info(f"Error posting response from {paperview_job_id}")
            raise e
            
    else:
        logger.info('partner status failed')    
        return {'message': 'parnter status failed'}
        
