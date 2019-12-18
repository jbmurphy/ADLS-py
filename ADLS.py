import os
import json

from azure.core import MatchConditions
from azure.storage.filedatalake import (
    DataLakeServiceClient,
)

def run():
    account_name = ""
    account_key = ""
    account_url="{}://{}.dfs.core.windows.net".format("https",account_name)
    fs_name = ""
    userName = ""
    sourceDir=""

    service_client = DataLakeServiceClient(account_url, credential=account_key)
    print()
    print()
    # Using existing file system (Admin created)
    print(" - Finding a filesystem named '{}'.".format(fs_name))
    filesystem_client = service_client.get_file_system_client(file_system=fs_name)

    # Creating a folder based on user name

    print(" - Creating a directory named '{}'.".format(userName))
    directory_client = filesystem_client.create_directory(userName,content_settings=None, metadata={'Source': 'rail_data','sourceUrl':'http://127.0.0.1?13456789'})
    
    # Set permissions on folder for XID
    acl="user::rwx,user:{}@company.com:rwx,group::r-x,mask::rwx,other::---,default:user::rwx,default:user:{}@company.com:rwx,default:group::r-x,default:mask::rwx,default:other::---".format(userName,userName)
    print(" - Setting permissions on named '{}'.".format(userName))
    directory_client.set_access_control(owner=None, group=None, permissions=None,acl=acl)
    
    # uploading all files in a directory

    print(" - Uploading all files in directory")
    
    for file_name in os.listdir(sourceDir):
        print("     - Opening a file named '{}'.".format(file_name))
        data = open("{}\\{}".format(sourceDir,file_name),"r")
        print("     - Uploading a file named '{}'.".format(file_name))
        file_client = directory_client.create_file(file_name,content_settings=None,metadata={'SourceFileName': file_name})
        data = data.read()
        file_client.append_data(data, offset=0, length=len(data))
        file_client.flush_data(len(data))
        print("     - Finished uploading '{}'.".format(file_name))

    print(" - Finished uploading all files in directory")
    
    # create a PBIDS file

    data = {
        "version": "0.1"
    }
    data['connections'] = []
    data['connections'].append({
    'details': {
        'protocol': 'azure-data-lake-storage',
        "address": {
            'server': '',
            "path": ''
        }
    },
    "options": {}, 
    "mode": "Import"
    })
    data['connections'][0]['details']['address'].update({'server': account_url})
    data['connections'][0]['details']['address'].update({'path':"/{}/{}".format(fs_name,userName)})
    print(" - Creating PBIDS file: '{}'.".format(userName + '.PBIDS'))

    with open(userName + '.PBIDS', 'w') as outfile:
        json.dump(data, outfile)

if __name__ == '__main__':
    run()
