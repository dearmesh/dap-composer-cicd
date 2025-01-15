from google.cloud import storage
import subprocess
import os
import pandas as pd
from google.cloud import bigquery
import re
import argparse

client = bigquery.Client.from_service_account_json('/root/key/sa-key.json')
dict_file = {}
dict_file["table_name"] = []
dict_file["gcs_file_path"] = []
dict_file["hdfs_file_path"] = []
dict_file["md5_gcs_file"] = []
dict_file["md5_hdfs_file"] = []
dict_file["gcs_command"] = []
dict_file["hdfs_command"] = []
#table = "dm_channel_trx_atm_off_us_new"
#pattern = r"date_pr=[\d/_]+"
pattern = r"date_pr=[\w\d\/\.-]+"
def list_blobs(bucket_name, prefix,table,source_path):
    """Lists all the blobs in the bucket."""

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name) 

    # Construct a query to list blobs and their sizes
    blobs = bucket.list_blobs(prefix=prefix)
    #print(blobs)
    #print(bucket_name,prefix)
    #dict_file = {}
    #dict_file["gcs_file"] = []
    #dict_file["md5_gcs_file"] = []
    #dict_file["md5_hdfs_file"] = []
    #dict_file["gcs_command"] = []
    #dict_file["hdfs_command"] = []
    for blob in blobs:
        filename="gs://dap-landing-bucket/"+blob.name
        dict_file["gcs_file_path"].append(filename)
        #print("blob_name : ",blob.name)
        #print(re.search(pattern,blob.name).group())

        print("Processing Table : ",table," And File Name :",filename.split(table)[1][1:])
        #check md5 on gcs
        out = subprocess.check_output("gsutil cat "+filename+"|md5sum",shell=True)
        #print("command gcs","gsutil cat "+filename+"|md5sum")
        dict_file["gcs_command"].append("gsutil cat "+filename+"|md5sum")
        dict_file["md5_gcs_file"].append(out.decode('utf-8')[:-2])
        #check md5 on hdfs
        command = f'curl -sL "http://clo-uat-edl201.corpuat.danamon.co.id:9870/webhdfs/v1{source_path}{re.search(pattern,blob.name).group()}?op=OPEN&delegation=$deleg"|md5sum'
        #print("command hdfs",command)
        dict_file["hdfs_file_path"].append(f'{source_path}{re.search(pattern,blob.name).group()}')
        dict_file["hdfs_command"].append(command)
        out_hdfs = subprocess.check_output(command, shell=True)
        dict_file["md5_hdfs_file"].append(out_hdfs.decode('utf-8')[:-2])
        #print(out_hdfs.decode('utf-8')[:-2])i
        dict_file['table_name'] = table


if __name__ == '__main__':
    bucket_name = "dap-landing-bucket"
    prefix = "/bd_tableau.db/dm_channel_trx_atm_off_us_new/"
    table = "dm_channel_trx_atm_off_us_new"
    source_path = "/warehouse/tablespace/external/hive/bd_tableau_dev.db/dm_channel_trx_atm_off_us_new/"
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--schema", type=str, required=True, help="Schema source")
    args = parser.parse_args()
    print("Schema yang dijalankan : ",args.schema)
    query = f" \
    SELECT * \
    FROM `prj-7810ed85d543e33a.udf_dap.master_table_path` \
    WHERE 1=1 \
    and enable_flag = 'Y' \
    and schema = '{args.schema}' \
    -- and table_name = 'lu_bank' \
    "

    query_job = client.query(query)  # API request - starts the query
    results = query_job.result()  # Waits for query to finish

    for row in results:
        #print(row[0])
        #print(row[1])
        #print(row[2])
        list_blobs(bucket_name, row[2][1:],row[0],row[1])
    #os.system("gsutil ls gs://dap-landing-bucket/newmisplus2.db/bd_ch_nobook/")
    #print(dict_file)
    df = pd.DataFrame(dict_file)
    df['md5_match'] = df['md5_gcs_file'] == df['md5_hdfs_file']
    print(df)
    client = bigquery.Client.from_service_account_json('/root/key/sa-key.json')
    df.to_gbq(destination_table='prj-7810ed85d543e33a.udf_dap.list_checksum',if_exists='replace',location='asia-southeast2')