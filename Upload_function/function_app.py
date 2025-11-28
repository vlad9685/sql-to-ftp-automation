import logging
import os
import io
import ftplib
import pandas as pd
import pyodbc
import datetime
import azure.functions as func
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

azure_logger = logging.getLogger("azure")
azure_logger.setLevel(logging.WARNING)  
uvicorn_logger = logging.getLogger("uvicorn")
uvicorn_logger.setLevel(logging.WARNING)  

app = func.FunctionApp()
logging.info("FunctionApp object created successfully.")


# Configuration
# Read settings from environment variables. These must be set in local.settings.json
SQL_CONNECTION_STRING = os.environ.get("SqlConnectionString")
STORAGE_ACCOUNT_URL = os.environ.get("StorageAccountUrl")
STORAGE_CONTAINER_NAME = os.environ.get("StorageContainerName")

class FtpUploadProgress:
    def __init__(self, total_size):
        self.total_size = total_size
        self.uploaded = 0
        self.last_logged_percent = -10

    def __call__(self, block):
        self.uploaded += len(block)
        percent_done = int((self.uploaded / self.total_size) * 100)
        
        # Log progress every 10%
        if percent_done >= self.last_logged_percent + 10:
            logging.info(f"FTP Upload Progress: {percent_done}%")
            self.last_logged_percent = percent_done

def get_upload_metadata_from_sql():
    """
    Connects to the SQL database and retrieves a DataFrame containing the
    mapping of CSV files to their FTP server destinations.
    
    Returns:
        pandas.DataFrame: A DataFrame with the combined metadata, or None if an error occurs.
    """
    if not SQL_CONNECTION_STRING:
        logging.error("SqlConnectionString is not set in the application settings.")
        return None

    try:
        conn = pyodbc.connect(SQL_CONNECTION_STRING, autocommit=True)
        logging.info("Successfully connected to SQL database.")
        
        # SQL query to join the three metadata tables
        query = """
        SELECT
            et.CsvFileName,
            fs.Host,
            fs.Port,
            fs.Protocol,
            fs.UsernameSecretName,
            fs.PasswordSecretName
        FROM ftp_export.ExportTables AS et
        JOIN ftp_export.ExportTableToFtpMapping AS etm ON et.ExportTableId = etm.ExportTableId
        JOIN ftp_export.FtpServers AS fs ON etm.FtpServerId = fs.FtpServerId
        WHERE et.IsActive = 1 AND etm.IsActive = 1 AND fs.IsActive = 1
        ORDER BY etm.Priority;
        """
        
        df = pd.read_sql(query, conn)
        conn.close()
        return df
        
    except Exception as e:
        logging.error(f"Error connecting to or querying SQL database: {e}")
        return None


def upload_to_ftp(host, port, username, password, file_stream, remote_filename):

   # Uploads a file stream to an FTP server with progress reporting.

    try:
        with ftplib.FTP() as ftp:
            ftp.connect(host, port)
            ftp.login(username, password)
            
            file_stream.seek(0)
            total_size = len(file_stream.getvalue())
            progress_tracker = FtpUploadProgress(total_size)
            
            ftp.storbinary(f'STOR {remote_filename}', file_stream, callback=progress_tracker)
            
            logging.info(f"Successfully uploaded '{remote_filename}' to {host}.")
            return True
    except Exception as e:
        logging.error(f"Failed to upload to FTP server {host}. Error: {e}")
        return False


@app.schedule(schedule="0 */5 * * * *", 
              arg_name="myTimer",
              run_on_startup=True) 
def ftp_uploader(myTimer: func.TimerRequest) -> None:
    """
    Orchestrates the process of reading metadata, fetching files from blob storage,
    and uploading them to the correct FTP servers.
    """
    logging.info("Decorator discovered. Registering the 'ftp_uploader' function.")
    utc_timestamp = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc).isoformat()
    logging.info(f'FTP Uploader function triggered at {utc_timestamp}.')

    if myTimer.past_due:
        logging.warning('The timer is past due!')

    # 1. Get the mapping of CSV files to FTP servers from the SQL database
    upload_metadata = get_upload_metadata_from_sql()
    if upload_metadata is None:
        logging.error("Halting execution due to an error fetching SQL metadata.")
        return

    if upload_metadata.empty:
        logging.info("No active upload metadata found in the database. Nothing to process.")
        return

    # 2. Connect to Azure Blob Storage and process files
    if not STORAGE_ACCOUNT_URL or not STORAGE_CONTAINER_NAME:
        logging.error("StorageAccountUrl or StorageContainerName is not set in Application Settings.")
        return
        
    try:
        credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(account_url=STORAGE_ACCOUNT_URL, credential=credential)
        container_client = blob_service_client.get_container_client(STORAGE_CONTAINER_NAME)
        
        logging.info(f"Successfully connected to container '{STORAGE_CONTAINER_NAME}'.")
        
        files_to_process = list(container_client.list_blobs())

        if not files_to_process:
            logging.info("No files found in the container to process.")
            return

        for blob in files_to_process:
            csv_filename = os.path.basename(blob.name)
            
            # Find the corresponding FTP details for this file from the SQL metadata
            target_servers = upload_metadata[upload_metadata['CsvFileName'] == csv_filename]
            
            if target_servers.empty:
                logging.warning(f"No active FTP mapping found for file '{csv_filename}'. Skipping.")
                continue

            # Download the blob content into memory
            blob_client = container_client.get_blob_client(blob)
            downloader = blob_client.download_blob()
            file_content_bytes = downloader.readall()

            # Upload to each target server defined in the metadata
            for index, server in target_servers.iterrows():
                ftp_user_secret = os.environ.get(server['UsernameSecretName'])
                ftp_pass_secret = os.environ.get(server['PasswordSecretName'])

                if not ftp_user_secret or not ftp_pass_secret:
                    logging.error(f"FTP credentials for '{server['UsernameSecretName']}' not found in Application Settings. Skipping upload for {server['Host']}.")
                    continue

                file_stream = io.BytesIO(file_content_bytes) # Create a new stream for each upload
                
                upload_successful = upload_to_ftp(
                    host=server['Host'],
                    port=int(server['Port']),
                    username=ftp_user_secret,
                    password=ftp_pass_secret,
                    file_stream=file_stream,
                    remote_filename=csv_filename
                )

                if upload_successful:
                    logging.info(f"Confirmation: Upload process for '{csv_filename}' to {server['Host']} completed successfully.")
                else:
                    logging.error(f"Confirmation: Upload process for '{csv_filename}' to {server['Host']} failed. Check previous logs for details.")

    except Exception as e:
        logging.error(f"An unexpected error occurred in the main process: {e}")

