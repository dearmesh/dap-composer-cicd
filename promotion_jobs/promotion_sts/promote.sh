# Contoh call
#sh promote.sh prj-37255c396cdac09b agent-cloudera-dap

filename="list_job_sts.csv"

# Set the delimiter (comma by default)
IFS=','


# Validation parameter should be 1
if [ $# != 2 ]; then # Check if less than 1 arguments are provided
  echo "Tambahkan parameter project_id prj-xxxxxxxxxx : $0 <project_id> <source-agent-pool>"
  exit 1 # Exit with error code
fi

echo "Set Project for "$1
gcloud config set project $1

# List looping for job
list_loop=(incremental initial)

sed '1d' "$filename" | while IFS=',' read -r schema table_name source_path destination_path; do
  # Process each row here
  for list in "${list_loop[@]}" ; do
    echo "==============================================================================================================================================================="
    job_name="sts_"$schema"_"$table_name"_"$list
    echo "Creating Job $job_name"
    if [ $list == "initial" ]; then 
        gcloud transfer jobs create hdfs://$source_path gs:/$destination_path --source-agent-pool=$2 --name $job_name --do-not-run --overwrite-when=different --delete-from=destination-if-unique --log-actions=copy,delete --log-action-states=succeeded,failed    
    fi
    if [ $list == "incremental" ]; then 
        gcloud transfer jobs create hdfs://$source_path gs:/$destination_path --source-agent-pool=$2 --name $job_name --do-not-run --overwrite-when=different --include-modified-after-relative 7776000s --log-actions=copy,delete --log-action-states=succeeded,failed 
    fi
    #script_create_job="gcloud transfer jobs create hdfs://"$source_path" gs:/"$destination_path" --source-agent-pool=agent-cloudera-dap --name "$job_name" --do-not-run --overwrite-when=different --delete-from=destination-if-unique --log-actions=copy,delete --log-action-states=succeeded,failed"
    #echo $script_create_job
    #gcloud transfer jobs create hdfs://$source_path gs:/$destination_path --source-agent-pool=agent-cloudera-dap --name $job_name --do-not-run --overwrite-when=different --delete-from=destination-if-unique --log-actions=copy,delete --log-action-states=succeeded,failed
    echo "===============================================================================================================================================================" 
  done
done