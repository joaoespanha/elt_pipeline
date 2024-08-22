import os
import logging
import shutil


def move_csv_file(csv_file, destination):
    try:
        # Add 'ex' to the start of the file name
        base_name = os.path.basename(csv_file)
        new_name = "x" + base_name
        new_path = os.path.join(destination, new_name)

        logging.info(f"Renaming file from {base_name} to {new_name}")

        # Move the file to the new directory with the new name
        shutil.move(csv_file, new_path)

        logging.info(f"CSV file successfully moved to {new_path}")

    except Exception as e:
        logging.error(f"An error occurred while moving the file: {e}")
        raise  # Re-raise the exception to handle it later if necessar


# Function to check if there are any files in the directory and return the first file path
def check_for_files(directory_path):
    # Check if the directory exists
    if not os.path.exists(directory_path):
        logging.info(f"Directory {directory_path} does not exist.")
        return None

    # Iterate through the items in the directory
    for item in os.listdir(directory_path):
        # Create the full path
        item_path = os.path.join(directory_path, item)
        # Check if the item is a file
        if os.path.isfile(item_path):
            return item_path  # Return the first file path found

    logging.info(f"No files found in directory {directory_path}.")
    return None
