import azure.functions as func
import logging
from zipfile import ZipFile
from azure.storage.blob import BlobServiceClient
import os
import yaml
import re
from cf1 import runCF
import time


app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="stage1/input/{name}",
                               connection="azureWebStorage") 
def blop_trigger(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    connection_string = os.environ['AzureWebJobsStorage']

    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage1.yaml"), "r") as f:
        stage1_config = yaml.load(f, Loader=yaml.FullLoader)

    stage1_input_dir = stage1_config["directories"]["input"]
    stage1_output_dir = stage1_config["directories"]["output"]

    stage1_bucket_input_folder = stage1_config["bucket-directories"]["input"]
    stage1_bucket_errors_folder = stage1_config["bucket-directories"]["errors"]
    stage2_bucket_folder = stage1_config["bucket-directories"]["output"]

    ### (2) Apply trigger filter (based on file name/path & extension) #####################
    # File name pattern: stage1/input/*.zip
    if not re.search(r"\A" + stage1_bucket_input_folder + r"/.*\.zip\Z", myblob.name, re.IGNORECASE):
        return
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1")
    blop_name = myblob.name[(myblob.name.find('input/')):]#samplecontainer/input/LAP1027.zip
    blob_client = input_container.get_blob_client(blop_name)
    zipName =  blop_name.replace('input/','')

    ### (3) Set up /tmp/stage1 tree structure ##############################################
    logging.info(f"Processing file: {blop_name}")

    if not os.path.exists(stage1_input_dir): os.makedirs(stage1_input_dir)
    if not os.path.exists(stage1_output_dir): os.makedirs(stage1_output_dir)

    zip_file_on_tmp = os.path.join("/tmp/", myblob.name)

    logging.info(f"zip Path: {zip_file_on_tmp}")
    #Downloading Zip to local system
    with open(file=zip_file_on_tmp, mode="wb") as sample_blob:
        download_stream = blob_client.download_blob()
        sample_blob.write(download_stream.readall())

    try:
        logging.info(f"############ START: Stage 1 for zip file: {zip_file_on_tmp} ############")
        output_dir = runCF(stage1_config, zip_file_on_tmp)
        logging.info(f"Output dir: {output_dir}")

        fileList = os.listdir(output_dir)
        for filename in fileList:
            logging.info(f"FilesList: {filename}")
            container_client_upload = blob_service_client.get_container_client(container="stage1/tmp")
            blob_client_upload = container_client_upload.get_blob_client(filename)

            f = open(output_dir+'\\'+filename, 'r')
            byt = f.read()
            blob_client_upload.upload_blob(byt, blob_type="BlockBlob")

        # Note: A directory can't be created atomically in a bucket. So instead of using a
        # created directory as the stage 2 trigger, we use a single "cf2.trigger.txt" file
        #container_client_upload.from_connection_string(conn_str="<connection_string>", container_name="my_container", blob_name="my_blob")
        blob = container_client_upload.get_blob_client("cf2.trigger.txt")
        blob.upload_blob("trigger me")

        print(f"############ END: Stage 1 for zip file: {zip_file_on_tmp} ############")
    except:
        logging.info(f"############ ERROR: Stage 1 for zip file: {zip_file_on_tmp} ############")
    """
    # Delete stage 1 zip file
    blob = input_container.get_blob_client(blop_name)     

    if blob.exists():
        logging.debug(f"Deleting bucket file: {blop_name}")
        blob.delete_blob()
    #
    with ZipFile(zip_file_on_tmp, 'r') as zip_ref:
    zip_ref.extractall(stage1_output_dir)
    """