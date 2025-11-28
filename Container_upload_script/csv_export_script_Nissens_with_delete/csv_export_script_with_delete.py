import pandas as pd
import pyodbc
from azure.storage.blob import BlobServiceClient
from azure.identity import DefaultAzureCredential
from datetime import datetime
import os
from dotenv import load_dotenv
import warnings
import re

# Load environment variables from .env file
load_dotenv()

warnings.filterwarnings('ignore', message='pandas only supports SQLAlchemy')

def delete_old_blobs_by_age(blob_service_client, container_name, days_to_keep=3):
    """Delete blobs older than specified days"""
    from datetime import datetime, timedelta
    
    try:
        container_client = blob_service_client.get_container_client(container_name)
        
        # Calculate cutoff date
        cutoff_date = datetime.now() - timedelta(days=days_to_keep)
        
        # List all blobs in the container
        blob_list = container_client.list_blobs()
        deleted_count = 0
        kept_count = 0
        
        # Delete blobs older than cutoff date
        for blob in blob_list:
            if blob.last_modified.replace(tzinfo=None) < cutoff_date:
                print(f"Deleting old blob: {blob.name} (last modified: {blob.last_modified})")
                blob_client = container_client.get_blob_client(blob.name)
                blob_client.delete_blob()
                deleted_count += 1
            else:
                print(f"Keeping recent blob: {blob.name} (last modified: {blob.last_modified})")
                kept_count += 1
        
        print(f"Deleted {deleted_count} old files and kept {kept_count} recent files")
        return True
    except Exception as e:
        print(f"Error deleting old blobs: {e}")
        return False
    
def main():
    print("Starting export process...")
    
    # Get connection info from environment variables
    server = os.getenv('SQL_SERVER')
    database = os.getenv('SQL_DATABASE')
    #username = os.getenv('SQL_USERNAME')
    #password = os.getenv('SQL_PASSWORD')
    storage_account = os.getenv('AZURE_STORAGE_ACCOUNT', 'saftpfuncdpnisneudev')
    container_name = os.getenv('AZURE_CONTAINER_NAME', 'exports')
    
    # Check if required variables are set
    if not all([server, database]):
        print("Error: Missing required environment variables!")
        print("Please set: SQL_SERVER, SQL_DATABASE")
        return
    
    # Build connection string
    connection_string = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"Authentication=ActiveDirectoryInteractive;"
    )
    
    print(f"Connecting to database: {server}/{database} using Active Directory Interactive authentication")
    print(f"Using storage account: {storage_account}")
    
    # Connect to SQL database
    try:
        conn = pyodbc.connect(connection_string)
        print("Connected to database successfully")
    except Exception as e:
        print(f"Error connecting to database: {e}")
        return
    
    # Connect to Azure Storage
    try:
        connection_string = os.getenv('AZURE_STORAGE_CONNECTION_STRING')
        if not connection_string:
            print("Error: AZURE_STORAGE_CONNECTION_STRING not found in environment variables")
            return
        
        blob_service_client = BlobServiceClient.from_connection_string(connection_string)
        print("Connected to Azure Storage successfully")
    except Exception as e:
        print(f"Error connecting to Azure Storage: {e}")
        return
    
    # Delete files older than 3 days from the container 
    print(f"\nCleaning up files older than 3 days from container '{container_name}'...")
    if not delete_old_blobs_by_age(blob_service_client, container_name, days_to_keep=3):
        print("Warning: Failed to delete some old files, but continuing with export...")

    # Get list of tables to export
    try:
        query = """
        SELECT ExportTableId, SchemaName, TableName, CsvFileName
        FROM ftp_export.ExportTables 
        WHERE IsActive = 1
        """
        tables_to_export = pd.read_sql(query, conn)
        print(f"Found {len(tables_to_export)} tables to export")
    except Exception as e:
        print(f"Error getting table list: {e}")
        return
    
    # Loop through each table and export it
    for index, row in tables_to_export.iterrows():
        table_id = row['ExportTableId']
        schema = row['SchemaName']
        table = row['TableName']
        csv_name = row['CsvFileName']
        
        print(f"Processing table: {schema}.{table}")
        
        try:
            # Get data from table
            table_query = f"SELECT * FROM [{schema}].[{table}]"
            data = pd.read_sql(table_query, conn)
            print(f"Got {len(data)} rows from {schema}.{table}")
            
            # Make filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if csv_name and csv_name.strip():
                base_name = csv_name.replace('.csv', '')
                
                base_name = re.sub(r'_\d{8}_\d{6}$', '', base_name)
                
                filename = f"{base_name}_{timestamp}.csv"
            else:
                filename = f"{schema}_{table}_{timestamp}.csv"
            
            print(f"Creating file: {filename}")
            
            # Convert to CSV
            csv_data = data.to_csv(index=False)
            
            # Upload to Azure
            blob = blob_service_client.get_blob_client(container=container_name, blob=filename)
            blob.upload_blob(csv_data, overwrite=True)
            print(f"Uploaded {filename} to Azure Storage")
            
            # Update last exported time and CsvFileName
            update_query = "UPDATE ftp_export.ExportTables SET LastExportedAt = GETDATE(), CsvFileName = ? WHERE ExportTableId = ?"
            cursor = conn.cursor()
            cursor.execute(update_query, filename, table_id)
            conn.commit()
            print(f"Updated timestamp and filename to {filename} for {schema}.{table}")
            
        except Exception as e:
            print(f"Error processing {schema}.{table}: {e}")
            continue
    
    conn.close()
    print("Export process finished!")

if __name__ == "__main__":
    main()














