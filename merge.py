import os
import shutil
import logging
import re
import time
from stage1 import readFoldersAndJoin,createCurrentOrders,createInstansitItems,createDigitalTransformation,createPlantMaterial


def merge(config, foldersPath):

    mergeDir = config["directories"]["merges"]

    logging.info(f"Merge Directory: {mergeDir}")
    logging.info(f"Folders Path: {foldersPath}")

    timestamp = str(int(time.time() * 1000))

    final_directory = os.path.join(mergeDir,timestamp)
    if not os.path.exists(final_directory): os.makedirs(final_directory)

    try:
        logging.info("Calling readFoldersAndJoin")
        folderName = readFoldersAndJoin(foldersPath)
    except:
        raise Exception('Was nos posible to extract/join the data')

    for files in os.listdir(foldersPath):
        full_path = os.path.join(foldersPath,files)
        if re.search(r"\.csv\Z", files, re.IGNORECASE):
            shutil.move(full_path, final_directory)

    return final_directory

def setCurrentOrder(config,workDirectory):

    currentOrder = config["directories"]["currentOrder"]
    timestamp = str(int(time.time() * 1000))

    final_directory = os.path.join(currentOrder,timestamp)
    if not os.path.exists(final_directory): os.makedirs(final_directory)

    try:
        logging.info("Calling create CurrentOrder")
        createCurrentOrders(workDirectory,final_directory)
    except:
        raise Exception('Was nos posible to create CurrentOrder')

    return final_directory

def setPlantMaterial(config,workDirectory):
    
    try:
        logging.info("Calling create Plant Material")
        createPlantMaterial(workDirectory,True)
    except:
        raise Exception('Was nos posible to create Intransit Item')

def setIntransitItem(config,workDirectory):

    instransit = config["directories"]["instransit"]
    timestamp = str(int(time.time() * 1000))

    final_directory = os.path.join(instransit,timestamp)
    if not os.path.exists(final_directory): os.makedirs(final_directory)

    try:
        logging.info("Calling create Instransit Item")
        createInstansitItems(workDirectory,final_directory)
    except:
        raise Exception('Was nos posible to create Intransit Item')

    return final_directory

def setDigitalTransformation(config,workDirectory):

    digital = config["directories"]["digital"]
    timestamp = str(int(time.time() * 1000))

    final_directory = os.path.join(digital,timestamp)
    if not os.path.exists(final_directory): os.makedirs(final_directory)

    try:
        logging.info("Calling create Digital")
        createDigitalTransformation(workDirectory,final_directory)
    except:
        raise Exception('Was nos posible to create Digital')
    
    shutil.rmtree(workDirectory)

    return final_directory



if( __name__ == "__main__"):
    merge()
    setCurrentOrder()
    setIntransitItem()
    setDigitalTransformation()
    setPlantMaterial()