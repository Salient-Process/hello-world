import os
import re
import zipfile
import logging
import shutil


def process_zip_file(config, file_path, work_dir, output_dir):
    """
        a.	Unpack the zip file
        b.	Transform the input CSV files it contains into output CSV files, none of which should be > 300MB 
        c.	Places the output CSV files into output_dir
    """
    logging.info(f"Enter in Stage1 {file_path}")
    logging.info("Sample log entry from Stage 1 processing...")

    zip_file_name = os.path.basename(file_path)

    # Throw artificial error to debug error processing
    if re.search("error", zip_file_name, re.IGNORECASE):
        raise Exception("Simulated exception because the zip file name contains the word 'error'") 

    # Extract file and place in output_dir
    with zipfile.ZipFile(file_path, 'r') as f:
        f.extractall(work_dir)
    
    # Move any CSV file to stage2_csv_dir
    for path in os.listdir(work_dir):
        full_path = os.path.join(work_dir, path)

        if re.search(r"\.csv\Z", path, re.IGNORECASE):
            shutil.move(full_path, output_dir)

    return