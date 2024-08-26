import os
import logging


# Function to check if there are any files in the directory and return the list of file paths
def check_for_files(directory_path):
    # Check if the directory exists
    if not os.path.exists(directory_path):
        logging.info(f"Directory {directory_path} does not exist.")
        return None

    # Iterate through the items in the directory
    files = []
    for item in os.listdir(directory_path):
        # Create the full path
        item_path = os.path.join(directory_path, item)
        # Check if the item is a file
        if os.path.isfile(item_path):
            files.append(item_path)

    if len(files) == 0:
        logging.info(f"No files found in directory {directory_path}.")
        return None

    return files
