import json
import requests
import sys
import os
from xml.dom import minidom


sasUrl = sys.argv[1]
releaseVersion = sys.argv[2].split(' ')[2]
print('Release Version: ' + releaseVersion)
if(len(releaseVersion)==0):
    print('Incorrect Release Version')
    sys.exit(1)

# Split the SAS URL to get the container URL and SAS token
containerUrl = sasUrl.split('?')[0]
sasToken = sasUrl.split('?')[1]

# Create a file and write the release version to it
file_name = 'latest_version.txt'
with open(file_name, 'w') as file:
    file.write(releaseVersion)
print(f'Data written to {file_name}')

# Get the full URL to upload the file
putUrl = containerUrl + '/' + file_name + '?' + sasToken

# Upload the file using a PUT request
with open(file_name, 'rb') as data:
    headers = {
        'x-ms-blob-type': 'BlockBlob',
        'Content-Length': str(os.path.getsize(file_name))
    }
    resp = requests.put(putUrl, data=data, headers=headers)
    
# Check if the request was successful
if resp.status_code < 200 or resp.status_code > 202:
    print(f"Failed to upload file: {resp.status_code}, {resp.text}")
    sys.exit(1)
else:
    print('File successfully uploaded')

# Clean up the local file
os.remove(file_name)
print(f"Local file '{file_name}' deleted.")