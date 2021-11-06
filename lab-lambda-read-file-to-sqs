import json
import boto3
import os
import csv
import codecs
import sys

s3 = boto3.resource('s3')


bucket = os.environ['bucket']
key = os.environ['key']
sqsName = os.environ['sqsname']
sqs = boto3.client('sqs')
sqsUrl = sqs.get_queue_url(QueueName= sqsName)
queue_url = sqsUrl['QueueUrl']
print(queue_url)


def lambda_handler(event, context):
   
   #
   #get() no se almacena en memoria
   try:
       obj = s3.Object(bucket, key).get()['Body']
   except:
       print("Error S3 No se pudo abrir el objeto. Verifique la variable de entorno.")
  

   batch_size = 10
   batch = []

   #DictReader es un generador; no almacenado en la memoria
   for row in csv.DictReader(codecs.getreader('utf-8')(obj)):
      if len(batch) >= batch_size:
         print(str(row))
         write_to_sqs(batch)
         batch.clear()

      batch.append(row)

   if batch:
      print(str(row))
      write_to_sqs(batch)

   return {
      'statusCode': 200,
      'body': json.dumps('Uploaded to SQS')
      }




def write_to_sqs(rows):
   try:
      for i in range(len(rows)):
         response = sqs.send_message(
         QueueUrl=queue_url,
         DelaySeconds=10,
         MessageAttributes={
             'Title': {
                  'DataType': 'String',
                     'StringValue': str(i) },
             },
         MessageBody=(str(rows))
         )
   except:
      print("Error al ejecutar sqs")
print('done')
