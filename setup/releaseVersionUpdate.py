import json
import requests
import sys
import os
from xml.dom import minidom
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient


sasUrl = sys.argv[1]
releaseVersion = sys.argv[2].split(' ')[2]
print('Release Version: ' + releaseVersion)
if(len(releaseVersion)==0):
    print('Incorrect Release Version')
    sys.exit(1)

containerUrl = sasUrl.split('?')[0]
sasToken = sasUrl.split('?')[1]

# delete latest version file in the container
# deleteUrl = containerUrl + '/' + 'latest_version.txt' + '?' + sasToken
# print(f"deleteURL: '{deleteUrl}'")
# resp = requests.delete(deleteUrl)
# sys.exit(1) if(resp.status_code<200 or resp.status_code>202) else print('Deleted last release file')

# Create a file and write data to it
file_name = 'latest_version.txt'

# Open the file in write mode
with open(file_name, 'w') as file:
    file.write(releaseVersion)
print(f'Data written to {file_name}')

# Create the BlobServiceClient using the SAS URL
blob_service_client = BlobServiceClient(account_url=sasUrl.split('?')[0], credential=sasUrl.split('?')[1])

# Get the container name from the SAS URL
container_name = sasUrl.split('?')[0].split('/')[-1]

# Get a client to interact with the container
container_client = blob_service_client.get_container_client(container_name)

# Upload the file to the container
with open(file_name, 'rb') as data:
    container_client.upload_blob(name=file_name, data=data, overwrite=True)

print(f"File '{file_name}' successfully uploaded to '{container_name}' container.")

# Clean up the local file
os.remove(file_name)
print(f"Local file '{file_name}' deleted.")
