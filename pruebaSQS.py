import json
import boto3
import os
import csv
import codecs
import sys
 

# Create SQS client
sqs = boto3.client('sqs')

# Get URL for SQS queue
response = sqs.get_queue_url(QueueName='sqsLamdbaReadFile')

print(response['QueueUrl'])

queue_url = response['QueueUrl']
response = sqs.send_message(
    QueueUrl=queue_url,
    DelaySeconds=10,
    MessageAttributes={
        'Title': {
            'DataType': 'String',
            'StringValue': 'The Whistler'
        },
        'Author': {
            'DataType': 'String',
            'StringValue': 'John Grisham'
        },
        'WeeksOn': {
            'DataType': 'Number',
            'StringValue': '6'
        }
    },
    MessageBody=(
        'Information about current NY Times fiction bestseller for '
        'week of 12/11/2016.'
    )
)

print(response['MessageId'])
