import json
import boto3
import os
import csv
import codecs
import sys

s3 = boto3.resource('s3')
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')


bucket =  'testfilemc'
key = 'testfilecopia2.csv'
sqsUrl = sqs.get_queue_url(QueueName='sqsLamdbaReadFile')
queue_url = sqsUrl['QueueUrl']
print(queue_url)
#para escribir en dynamodb
def write_to_sqs(rows):
    #try:
        for i in range(len(rows)):
            print ("\n" + str(i))
            print (rows[i])
            print ("\n")
            response = sqs.send_message(
                        QueueUrl="https://sqs.us-west-2.amazonaws.com/481009461591/sqsLamdbaReadFile",
                        DelaySeconds=10,
                        MessageAttributes={
                            'Title': {
                                 'DataType': 'String',
                                    'StringValue': str(i) },
                            },
                        MessageBody=(str(rows))
                        )
    #except:
      #print("Error al ejecutar sqs")




try:
    obj = s3.Object(bucket, key).get()['Body']
except:
    print("Error S3 No se pudo abrir el objeto. Verifique la variable de entorno." )
#try:
#    table = dynamodb.Table(tableName)
#except:
#    print("Error al cargar la tabla DynamoDB. Compruebe si la tabla se creó correctamente y la variable de entorno.")

batch_size = 100
batch = []

#DictReader es un generador; no almacenado en la memoria
for row in csv.DictReader(codecs.getreader('utf-8')(obj)):
  if len(batch) >= batch_size:
     print(batch) 
     write_to_sqs(batch)
    # print ('\n')
     batch.clear()

  batch.append(row)

if batch:
  print (batch)
  write_to_sqs(batch)

print('done')


