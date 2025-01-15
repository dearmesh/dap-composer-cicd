# Contoh call 
# sh promote.sh prj-7810ed85d543e33a prj-38e2f8f49bccda2d 
filename="list_table_bq.csv"

# Set the delimiter (comma by default)
IFS=';'
region='asia-southeast2'
project_destination=$2
project_source=$1

# Validation parameter should be 1
if [ $# != 2 ]; then # Check if less than 2 arguments are provided
  echo "Tambahkan parameter project_id prj-xxxxxxxxxx : $0 <source_project_id> <destination_project_id>"
  exit 1 # Exit with error code
fi


echo "Set Project for Destination "$2
gcloud config set project $2

sed '1d' "$filename" | while IFS=$IFS read -r dataset table_name is_partition is_cluster cluster_by; do

    # Check dataset
    if bq ls --project_id=$project_destination $dataset > /dev/null 2>&1; then
    echo "Dataset $dataset tersedia."
    else
    echo "Dataset $dataset tidak ditemukan."
    echo "Membuat dataset baru."
    bq mk --dataset --max_time_travel_hours=168 --location=$region $project_destination:$dataset
    fi

    # Create table partitions and cluster
    if [ $is_cluster == "Y" ] && [ $is_partition == "Y" ]; then 
      query_create_table="create table \`$project_destination.$dataset.$table_name\` partition by business_date cluster by $cluster_by AS select * from \`$project_source.$dataset.$table_name\` WHERE 1=0"
      echo "Create table with partitions and cluster"
      bq query --use_legacy_sql=false $query_create_table
    fi

    # Create table partitions without cluster
    if [ $is_cluster == "N" ] && [ $is_partition == "Y" ]; then 
      query_create_table="create table \`$project_destination.$dataset.$table_name\` partition by business_date AS select * from \`$project_source.$dataset.$table_name\` WHERE 1=0"
      echo "Create table with partitions and cluster"
      bq query --use_legacy_sql=false $query_create_table
    fi

    # Create table without partitions and cluster
    if [ $is_cluster == "N" ] && [ $is_partition == "N" ]; then 
      query_create_table="create table \`$project_destination.$dataset.$table_name\` AS select * from \`$project_source.$dataset.$table_name\` WHERE 1=0"
      echo "Create table with partitions and cluster"
      bq query --use_legacy_sql=false $query_create_table
    fi
done