import os
import shutil
import logging
import re
import time
from stage1 import readFoldersAndJoin


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

    shutil.rmtree(foldersPath)

    return final_directory

if( __name__ == "__main__"):
    merge()