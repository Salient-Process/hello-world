import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
import os
import yaml
import re



app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="stage1/input/{name}",
                               connection="AzureWebJobsStorage")
def blop_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    connection_string = os.environ['AzureWebJobsStorage']