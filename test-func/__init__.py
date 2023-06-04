import logging
import os
import shutil
import tempfile
import zipfile
import azure.functions as func
from azure.storage.blob import BlobServiceClient
# from dotenv import load_dotenv
# load_dotenv('.env')

def unzip_encrypted_blob(source_file_name: str, memberson_directory: str) -> None:
    # Azure Blob Storage connection string
    connection_string = os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    logging.info(f"Azure Storage connection string: {connection_string}")

    # Create a BlobServiceClient object
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    # Get the blob container name
    container_name = "dataset"

    # Create a temporary directory to store the unzipped files
    temp_dir = tempfile.mkdtemp()

    try:
        # Get the container client
        container_client = blob_service_client.get_container_client(container_name)

        # Construct the blob path for the current file in the memberson directory
        blob_path = os.path.join(memberson_directory, source_file_name)

        # Log and print the blob path
        logging.info(f"Blob path: {blob_path}")
        print(f"Blob path: {blob_path}")

        # Download the zip file
        zip_file_path = os.path.join(temp_dir, source_file_name)
        with open(zip_file_path, "wb") as zip_file:
            download_stream = container_client.download_blob(blob_path)
            zip_file.write(download_stream.readall())

        # Get the unzip password from environment variables or secrets
        password = os.environ["UNZIP_PASSWORD"]
        logging.info(f'UNZIP_PASSWORD: {unzip_password}')

        # Unzip the file with the password
        with zipfile.ZipFile(zip_file_path, "r") as zip_ref:
            # Extract the contents to the temporary directory
            zip_ref.extractall(temp_dir, pwd=password.encode())

        # Get the unzipped file names
        unzipped_files = [
            file_name for file_name in os.listdir(temp_dir) if not file_name.endswith(".zip")
        ]

        # Process each file (replace with your processing logic)
        for file_name in unzipped_files:
            file_path = os.path.join(temp_dir, file_name)
            if os.path.isfile(file_path):
                # Process the file

                # ...

                # After processing, you can save the file to the memberson_directory/unzipped/source_file_name_without_extension directory

                # Get the source file name without the extension
                source_file_name_without_extension = os.path.splitext(source_file_name)[0]

                # Create the destination directory path
                destination_directory = os.path.join(temp_dir, memberson_directory, "unzipped", source_file_name_without_extension)

                # Create the destination directory if it doesn't exist or overwrite if it exists
                os.makedirs(destination_directory, exist_ok=True)

                # Move the file to the destination directory
                destination_file_path = os.path.join(destination_directory, file_name)
                shutil.move(file_path, destination_file_path)

                # Upload the processed file to the memberson_directory/unzipped/source_file_name_without_extension directory in the same Blob container
                new_blob_name = os.path.join(memberson_directory, "unzipped", source_file_name_without_extension, file_name)
                with open(destination_file_path, "rb") as file:
                    container_client.upload_blob(name=new_blob_name, data=file, overwrite=True)

    finally:
        # Delete the temporary directory and its contents
        shutil.rmtree(temp_dir)

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Received a request')

    try:
        req_body = req.get_json()
        logging.info(f'Request body: {req_body}')  # Add this line to log the received request body

        source_file_name = req_body["sourceFileName"]
        memberson_directory = req_body["membersonDirectory"]

        logging.info(f'Source file name: {source_file_name}')
        logging.info(f'Memberson directory: {memberson_directory}')

        unzip_encrypted_blob(source_file_name, memberson_directory)
        return func.HttpResponse(f"Successfully processed {source_file_name} in the memberson directory.")

    except Exception as e:
        logging.error(f"Error: {str(e)}")
        return func.HttpResponse("An error occurred during processing.", status_code=500)
