import azure.functions as func
import logging
from azure.storage.blob import BlobServiceClient
import os
import yaml
import re
import shutil
import time
import json
from script_util import convertDict
from extract import runCF
from merge import merge,setCurrentOrder,setIntransitItem,setDigitalTransformation


app = func.FunctionApp()

@app.blob_trigger(arg_name="myblob", path="stage1/input/{name}",
                               connection="AzureWebJobsStorage")
def blop_trigger(myblob: func.InputStream):
    
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    global output_dir
    connection_string = os.environ['AzureWebJobsStorage']
    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage1.yaml"), "r") as f:
        stage1_config = yaml.load(f, Loader=yaml.FullLoader)

    stage1_input_dir = stage1_config["directories"]["input"]
    stage1_output_dir = stage1_config["directories"]["output"]

    stage1_bucket_input_folder = stage1_config["bucket-directories"]["input"]
    stage1_bucket_errors_folder = stage1_config["bucket-directories"]["errors"]

    ### (2) Apply trigger filter (based on file name/path & extension) #####################
    # File name pattern: stage1/input/*.zip
    if not re.search(r"\A" + stage1_bucket_input_folder + r"/.*\.zip\Z", myblob.name, re.IGNORECASE):
        return
    
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1")
    blop_name = myblob.name[(myblob.name.find('input/')):]#samplecontainer/input/LAP1027.zip
    blob_client = input_container.get_blob_client(blop_name)

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
        data = {}
        data['output_dir'] = output_dir
        json_data = json.dumps(data)
        container_client_upload = blob_service_client.get_container_client(container="stage1/merge")
        # Note: A directory can't be created atomically in a bucket. So instead of using a
        # created directory as the stage 2 trigger, we use a single "cf2.trigger.txt" file
        #container_client_upload.from_connection_string(conn_str="<connection_string>", container_name="my_container", blob_name="my_blob")
        blob = container_client_upload.get_blob_client("merge.trigger.txt")
        blob.upload_blob(json_data)
        print(f"############ END: Stage 1 for zip file: {zip_file_on_tmp} ############")
    except:
        logging.info(f"############ ERROR: Stage 1 for zip file: {zip_file_on_tmp} ############")
    # Delete stage 1 zip file
    blob = input_container.get_blob_client(blop_name)     

    if blob.exists():
        logging.debug(f"Deleting bucket file: {blop_name}")
        blob.delete_blob()


@app.blob_trigger(arg_name="myblob", path="stage1/merge/{name}",
                               connection="AzureWebJobsStorage") 
def mergeFiles(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")

    connection_string = os.environ['AzureWebJobsStorage']
    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage1.yaml"), "r") as f:
        stage1_config = yaml.load(f, Loader=yaml.FullLoader)

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1/merge")
    blob = input_container.get_blob_client("merge.trigger.txt")
    data = convertDict(myblob)
   
    merge_directory = merge(stage1_config,data['output_dir'])
    
    data = {}
    data['merge_directory'] = merge_directory
    json_data = json.dumps(data)

    """
    fileList = os.listdir(merge_directory)
    for filename in fileList:
        logging.info(f"FilesList: {filename}")
        container_client_upload = blob_service_client.get_container_client(container="stage1/merge_directory")
        blob_client_upload = container_client_upload.get_blob_client(filename)

        f = open(merge_directory+'\\'+filename, 'r',encoding='utf-8')
        byt = f.read()
        blob_client_upload.upload_blob(byt, blob_type="BlockBlob")

    """
    container_client_upload = blob_service_client.get_container_client(container="stage1/current")
        # Note: A directory can't be created atomically in a bucket. So instead of using a
        # created directory as the stage 2 trigger, we use a single "cf2.trigger.txt" file
        #container_client_upload.from_connection_string(conn_str="<connection_string>", container_name="my_container", blob_name="my_blob")
    blobCurrent = container_client_upload.get_blob_client("current.trigger.txt")
    blobCurrent.upload_blob(json_data)

    if blob.exists():
        logging.debug(f"Deleting bucket file: merge.trigger.txt")
        blob.delete_blob()



@app.blob_trigger(arg_name="myblob", path="stage1/current/{name}",
                               connection="AzureWebJobsStorage") 
def createCurrentOrder(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    connection_string = os.environ['AzureWebJobsStorage']

    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage1.yaml"), "r") as f:
        stage1_config = yaml.load(f, Loader=yaml.FullLoader)

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1/current")
    blob = input_container.get_blob_client("current.trigger.txt")
    data = convertDict(myblob)  
    
    
    merge_directory = data['merge_directory']
    logging.info(f"Merge Directory: {merge_directory}")
    currentOrderDirectory = setCurrentOrder(stage1_config,merge_directory)

    if blob.exists():
        logging.debug(f"Deleting bucket file: current.trigger.txt")
        blob.delete_blob()
    
    dataf = {}
    dataf['currentOrderDirectory'] = currentOrderDirectory
    dataf['merge_directory'] = merge_directory
    json_data = json.dumps(dataf)

    container_client_upload = blob_service_client.get_container_client(container="stage1/intransit")
        # Note: A directory can't be created atomically in a bucket. So instead of using a
        # created directory as the stage 2 trigger, we use a single "cf2.trigger.txt" file
        #container_client_upload.from_connection_string(conn_str="<connection_string>", container_name="my_container", blob_name="my_blob")
    blobCurrent = container_client_upload.get_blob_client("intransit.trigger.txt")
    blobCurrent.upload_blob(json_data)


@app.blob_trigger(arg_name="myblob", path="stage1/intransit/{name}",
                               connection="AzureWebJobsStorage") 
def createIntransitItem(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    connection_string = os.environ['AzureWebJobsStorage']
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1/intransit")
    blob = input_container.get_blob_client("intransit.trigger.txt")
    data =  convertDict(myblob)
    merge_directory = data['merge_directory']

    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage1.yaml"), "r") as f:
        stage1_config = yaml.load(f, Loader=yaml.FullLoader)

    instransitDirectory = setIntransitItem(stage1_config,merge_directory)

    if blob.exists():
        logging.debug(f"Deleting bucket file: intransit.trigger.txt")
        blob.delete_blob()

    container_client_upload = blob_service_client.get_container_client(container="stage1/digital")
        # Note: A directory can't be created atomically in a bucket. So instead of using a
        # created directory as the stage 2 trigger, we use a single "cf2.trigger.txt" file
        #container_client_upload.from_connection_string(conn_str="<connection_string>", container_name="my_container", blob_name="my_blob")
    dataf = {}
    dataf['currentOrderDirectory'] = data['currentOrderDirectory']
    dataf['merge_directory'] = merge_directory
    dataf['instransitDirectory'] = instransitDirectory
    json_data = json.dumps(dataf)
    
    blobCurrent = container_client_upload.get_blob_client("digital.trigger.txt")
    blobCurrent.upload_blob(json_data)


@app.blob_trigger(arg_name="myblob", path="stage1/digital/{name}",
                               connection="AzureWebJobsStorage") 
def createDigital(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    data =  convertDict(myblob)
    merge_directory = data['merge_directory']

    connection_string = os.environ['AzureWebJobsStorage']
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1/digital")
    blob = input_container.get_blob_client("digital.trigger.txt")
    logging.info(f"Merge Directory: {merge_directory}")
    script_dir = os.path.dirname(os.path.abspath(__file__))
    with open(os.path.join(script_dir, "stage1.yaml"), "r") as f:
        stage1_config = yaml.load(f, Loader=yaml.FullLoader)

    digitalDirectory = setDigitalTransformation(stage1_config,merge_directory)

    if blob.exists():
        logging.debug(f"Deleting bucket file: digital.trigger.txt")
        blob.delete_blob()

    container_client_upload = blob_service_client.get_container_client(container="stage1/uploadCurrent")
        # Note: A directory can't be created atomically in a bucket. So instead of using a
        # created directory as the stage 2 trigger, we use a single "cf2.trigger.txt" file
        #container_client_upload.from_connection_string(conn_str="<connection_string>", container_name="my_container", blob_name="my_blob")
    
    dataf = {}
    dataf['currentOrderDirectory'] = data['currentOrderDirectory']
    dataf['merge_directory'] = merge_directory
    dataf['instransitDirectory'] = data['instransitDirectory']
    dataf['digitalDirectory'] = digitalDirectory
    json_data = json.dumps(dataf)
    
    blobCurrent = container_client_upload.get_blob_client("uploadCurrent.trigger.txt")
    blobCurrent.upload_blob(json_data)


@app.blob_trigger(arg_name="myblob", path="stage1/uploadCurrent/{name}",
                               connection="AzureWebJobsStorage") 
def uploadCurrentOrder(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    connection_string = os.environ['AzureWebJobsStorage']


    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1/uploadCurrent")
    blob = input_container.get_blob_client("uploadCurrent.trigger.txt")
    data = convertDict(myblob)
    currentOrderDirectory = data['currentOrderDirectory']
    logging.info(f"Current Order Directory: {currentOrderDirectory}")

    fileList = os.listdir(currentOrderDirectory)
    for filename in fileList:
        logging.info(f"FilesList: {filename}")
        container_client_upload = blob_service_client.get_container_client(container="stage2/currentOder")
        with open(file=os.path.join(currentOrderDirectory, filename), mode="rb") as data:
            blob_client = container_client_upload.upload_blob(name=filename, data=data, overwrite=True)

    if blob.exists():
        logging.debug(f"Deleting bucket file: digital.trigger.txt")
        blob.delete_blob()
    
    dataf = {}
    dataf['currentOrderDirectory'] = data['currentOrderDirectory']
    dataf['instransitDirectory'] = data['instransitDirectory']
    dataf['digitalDirectory'] = data['digitalDirectory']
    json_data = json.dumps(dataf)
    

    blob_client = blob_service_client.get_blob_client(container="stage1/uploadInstransit", blob="uploadInstransit.trigger.txt")
    input_stream = json_data
    blob_client.upload_blob(input_stream, blob_type="BlockBlob")

@app.blob_trigger(arg_name="myblob", path="stage1/uploadInstransit/{name}",
                               connection="AzureWebJobsStorage") 
def uploadIntransit(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    data = convertDict(myblob)
    instransitDirectory =  data['instransitDirectory']
    currentOrderDirectory =  data['currentOrderDirectory']

    connection_string = os.environ['AzureWebJobsStorage']
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1/uploadInstransit")
    blob = input_container.get_blob_client("uploadInstransit.trigger.txt")
    fileList = os.listdir(instransitDirectory)
    for filename in fileList:
        logging.info(f"FilesList: {filename}")
        container_client_upload = blob_service_client.get_container_client(container="stage2/Intransit")
        """
        blob_client_upload = container_client_upload.get_blob_client(filename)

        f = open(instransitDirectory+'\\'+filename, 'r',encoding='utf-8')
        byt = f.read()
        blob_client_upload.upload_blob(byt, blob_type="BlockBlob")
        """
        with open(file=os.path.join(instransitDirectory, filename), mode="rb") as data:
            blob_client = container_client_upload.upload_blob(name=filename, data=data, overwrite=True)

    if blob.exists():
        logging.debug(f"Deleting bucket file: digital.trigger.txt")
        blob.delete_blob()

    dataf = {}
    dataf['instransitDirectory'] = data['instransitDirectory']
    dataf['digitalDirectory'] = data['digitalDirectory']
    json_data = json.dumps(dataf)
    
    blob_client = blob_service_client.get_blob_client(container="stage1/uploadDigital", blob="uploadDigital.trigger.txt")
    input_stream = json_data
    blob_client.upload_blob(input_stream, blob_type="BlockBlob")

    shutil.rmtree(currentOrderDirectory)

@app.blob_trigger(arg_name="myblob", path="stage1/uploadDigital/{name}",
                               connection="AzureWebJobsStorage") 
def uploadDigital(myblob: func.InputStream):
    logging.info(f"Python blob trigger function processed blob"
                f"Name: {myblob.name}"
                f"Blob Size: {myblob.length} bytes")
    
    data = convertDict(myblob)
    instransitDirectory =  data['instransitDirectory']
    digitalDirectory =  data['digitalDirectory']
    
    connection_string = os.environ['AzureWebJobsStorage']
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)
    input_container = blob_service_client.get_container_client(container="stage1/uploadDigital")
    blob = input_container.get_blob_client("uploadDigital.trigger.txt")
    fileList = os.listdir(digitalDirectory)
    for filename in fileList:
        logging.info(f"FilesList: {filename}")
        container_client_upload = blob_service_client.get_container_client(container="stage2/Digital")
        """
        blob_client_upload = container_client_upload.get_blob_client(filename)

        f = open(digitalDirectory+'\\'+filename, 'r',encoding='utf-8')
        byt = f.read()
        blob_client_upload.upload_blob(byt, blob_type="BlockBlob")
        """
        with open(file=os.path.join(digitalDirectory, filename), mode="rb") as data:
            blob_client = container_client_upload.upload_blob(name=filename, data=data, overwrite=True)

    if blob.exists():
        logging.debug(f"Deleting bucket file: digital.trigger.txt")
        blob.delete_blob()
    
    blob_client = blob_service_client.get_blob_client(container="stage2", blob="sendToPrM.trigger.txt")
    input_stream = "trigger me"
    blob_client.upload_blob(input_stream, blob_type="BlockBlob")

    shutil.rmtree(instransitDirectory)                                                                                                                                       