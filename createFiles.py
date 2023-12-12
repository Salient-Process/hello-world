import os
import logging
import time
import shutil
from stage1 import createCurrentOrders,createInstansitItems,createDigitalTransformation

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

def setIntransitItem(config,workDirectory):

    instransit = config["directories"]["instransit"]
    timestamp = str(int(time.time() * 1000))

    final_directory = os.path.join(instransit,timestamp)
    if not os.path.exists(final_directory): os.makedirs(final_directory)

    try:
        logging.info("Calling create Instransit Item")
        createInstansitItems(workDirectory,final_directory)
    except:
        raise Exception('Was nos posible to create CurrentOrder')

    return final_directory

def setDigitalTransformation(config,workDirectory):

    digital = config["directories"]["digital"]
    timestamp = str(int(time.time() * 1000))

    final_directory = os.path.join(digital,timestamp)
    if not os.path.exists(final_directory): os.makedirs(final_directory)

    try:
        logging.info("Calling create Digital Item")
        createDigitalTransformation(workDirectory,final_directory)
    except:
        raise Exception('Was nos posible to create CurrentOrder')
    
    shutil.rmtree(workDirectory)

    return final_directory

if( __name__ == "__main__"):
    setCurrentOrder()
    setIntransitItem()
    setDigitalTransformation()