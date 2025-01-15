import re
from typing import Dict, Union, List, Any


class DataFileFormat:
    def __init__(self , file_pattern:str, file_type: str = 'CSV', header: bool = True, delimiter: str = ',', count_delimiter: int = None, retention_file_day: int = None, retention_table_day: int = None) -> None:
        self.file_type = file_type  # type: str
        self.header = header  # type: bool
        self.delimiter = delimiter  # type: str
        self.file_pattern = file_pattern  # type: str
        self.count_delimiter = count_delimiter  # type: int
        self.retention_file_day = retention_file_day  # type: int
        self.retention_table_day = retention_table_day  # type: int
        
        

    def __repr__(self):
        return repr(self.__dict__)

    @classmethod
    def from_dict(cls, file_config_dict: Dict[str, Union[str, bool,str,int, int]]) -> 'DataFileFormat':
        return cls(file_type=file_config_dict['file_type'],
                   header=file_config_dict['header'],
                   delimiter=file_config_dict['delimiter'],
                   file_pattern=file_config_dict['file_pattern'],
                   count_delimiter=file_config_dict['count_delimiter'],
                   retention_file_day=file_config_dict['retention_file_day'],
                   retention_table_day=file_config_dict['retention_table_day']
                   )

class BigQueryTable:
    def __init__(self, server: str, project_id: str, dataset_id: str, table_name: str,last_update_date_column_source:str,last_update_date_column_target:str , transaction_date_column_target:str,transaction_date_column_source:str,table_header_dependency:str,column_header_id_dependency:str,column_line_id_dependency:str ,additional_table_source:str,additional_table_target:str,additional_where_source:str,additional_where_target:str,schema_fields: str) -> None:
        self.server = server # type: str
        self.project_id = project_id  # type: str
        self.dataset_id = dataset_id  # type: str
        self.table_name = table_name  # type: str
        self.last_update_date_column_source = last_update_date_column_source # type: str
        self.last_update_date_column_target = last_update_date_column_target # type: str
        self.transaction_date_column_target = transaction_date_column_target # type str
        self.transaction_date_column_source = transaction_date_column_source # type str
        self.table_header_dependency = table_header_dependency # type str
        self.column_header_id_dependency = column_header_id_dependency # type str
        self.column_line_id_dependency = column_line_id_dependency # type str
        self.additional_table_source = additional_table_source # type str
        self.additional_table_target = additional_table_target # type str
        self.additional_where_source = additional_where_source # type str
        self.additional_where_target = additional_where_target # type str
        self.schema_fields = schema_fields  # type: str

    def __repr__(self):
        return repr(self.__dict__)

    @classmethod
    def from_dict(cls, bq_table_dict: Dict[str, Union[str, str, str,str,str,str,str,str,str,str,str,str,str,str,str]]) -> 'BigQueryTable':
        return cls(server=bq_table_dict['server'],
                   project_id=bq_table_dict['project_id'],
                   dataset_id=bq_table_dict['dataset_id'],
                   table_name=bq_table_dict['table_name'],
                   last_update_date_column_source=bq_table_dict['last_update_date_column_source'],
                   last_update_date_column_target=bq_table_dict['last_update_date_column_target'],
                   transaction_date_column_target=bq_table_dict['transaction_date_column_target'],
                   transaction_date_column_source=bq_table_dict['transaction_date_column_source'],
                   table_header_dependency=bq_table_dict['table_header_dependency'],
                   column_header_id_dependency=bq_table_dict['column_header_id_dependency'],
                   column_line_id_dependency=bq_table_dict['column_line_id_dependency'],
                   additional_table_source = bq_table_dict['additional_table_source'],
                   additional_table_target = bq_table_dict['additional_table_target'],
                   additional_where_source = bq_table_dict['additional_where_source'],
                   additional_where_target = bq_table_dict['additional_where_target'],
                   schema_fields=bq_table_dict['schema_fields']
                   )

class SqlOperation:
    def __init__(self, sql_insert_temp_ext_to_temp:str ,sql_check_dt_pr_exists:str ,sql_insert: str,sql_delete: str, sql_prefix_query_table:str ,processing_location: str = None) -> None:
        self.sql_insert_temp_ext_to_temp = sql_insert_temp_ext_to_temp  # type: str
        self.sql_check_dt_pr_exists = sql_check_dt_pr_exists  # type: str
        self.sql_insert = sql_insert  # type: str
        self.sql_delete = sql_delete  # type: str
        self.sql_prefix_query_table = sql_prefix_query_table
        self.processing_location = processing_location  # type: str

    def __repr__(self):
        return repr(self.__dict__)

    @classmethod
    def from_dict(cls, sql_operation_dict: Dict[str, Union[str, str, str, str]]) -> 'SqlOperation':
        return cls(sql_insert_temp_ext_to_temp=sql_operation_dict['sql_insert_temp_ext_to_temp'],
                   sql_check_dt_pr_exists=sql_operation_dict['sql_check_dt_pr_exists'],
                   sql_insert=sql_operation_dict['sql_insert'],
                   sql_delete=sql_operation_dict['sql_delete'],
                   sql_prefix_query_table=sql_operation_dict['sql_prefix_query_table'],
                   processing_location=sql_operation_dict['processing_location'])

class SpOperation:
    def __init__(self, sp_location:str, sp_dataset_id:str ,sp_key_column: str ,sp_validate_column: str ,sp_validate_duplicate: str,sp_check_dependency:str,sp_validate_master_job:str,sp_datamart_name: str ,sp_datamart_parameter: str, sp_check_data_retention : str, sp_data_cleansing_export : str, sp_delete_data_cleansing : str) -> None:
        self.sp_location = sp_location  # type: str
        self.sp_dataset_id = sp_dataset_id  # type: str
        self.sp_key_column = sp_key_column  # type: str
        self.sp_validate_column = sp_validate_column  # type: str
        self.sp_validate_duplicate = sp_validate_duplicate  # type: str
        self.sp_check_dependency = sp_check_dependency  # type: str
        self.sp_validate_master_job = sp_validate_master_job # type: str
        self.sp_datamart_name = sp_datamart_name  # type: str
        self.sp_datamart_parameter = sp_datamart_parameter  # type: str
        self.sp_check_data_retention = sp_check_data_retention  # type: str
        self.sp_data_cleansing_export = sp_data_cleansing_export  # type: str
        self.sp_delete_data_cleansing = sp_delete_data_cleansing  # type: str
        
        

    def __repr__(self):
        return repr(self.__dict__)

    @classmethod
    def from_dict(cls, sp_operation_dict: Dict[str, Union[str, str, str,str,str,str,str,str,str,str,str]]) -> 'SpOperation':
        return cls(sp_location=sp_operation_dict['sp_location'],
                   sp_dataset_id=sp_operation_dict['sp_dataset_id'],
                   sp_key_column=sp_operation_dict['sp_key_column'],
                   sp_validate_column=sp_operation_dict['sp_validate_column'],
                   sp_validate_duplicate=sp_operation_dict['sp_validate_duplicate'],
                   sp_check_dependency=sp_operation_dict['sp_check_dependency'],
                   sp_validate_master_job = sp_operation_dict['sp_validate_master_job'],
                   sp_datamart_name=sp_operation_dict['sp_datamart_name'],
                   sp_datamart_parameter=sp_operation_dict['sp_datamart_parameter'],
                   sp_check_data_retention=sp_operation_dict['sp_check_data_retention'],
                   sp_data_cleansing_export=sp_operation_dict['sp_data_cleansing_export'],
                   sp_delete_data_cleansing=sp_operation_dict['sp_delete_data_cleansing']
                   )

class STSOperation:
    def __init__(self, sts_project_id:str, sts_job_name:str ) -> None:
        self.sts_project_id = sts_project_id  # type: str
        self.sts_job_name = sts_job_name  # type: str

    def __repr__(self):
        return repr(self.__dict__)

    @classmethod
    def from_dict(cls, sts_operation_dict: Dict[str, str]) -> 'STSOperation':
        return cls(sts_project_id=sts_operation_dict['sts_project_id'],
                   sts_job_name=sts_operation_dict['sts_job_name']
                   )   

class GcsFolder:
    def __init__(self, bucket: str, folder: str) -> None:
        self.bucket = bucket  # type: str
        self.folder = folder  # type: str

    def __repr__(self):
        return repr(self.__dict__)

    @classmethod
    def from_dict(cls, gcs_config_dict: Dict[str, str]) -> 'GcsFolder':
        return cls(bucket=gcs_config_dict['bucket'], folder=gcs_config_dict['folder'])


class AdditionalParam:
    def __init__(self, additionalparam1:str, additionalparam2:str ,additionalparam3: str,additionalparam4: str,additionalparam5: str,additionalparam6: str,additionalparam7: str,additionalparam8: str,additionalparam9: str,additionalparam10: str ) -> None:
        self.additionalparam1 = additionalparam1  # type: str
        self.additionalparam2 = additionalparam2  # type: str
        self.additionalparam3 = additionalparam3  # type: str
        self.additionalparam4 = additionalparam4  # type: str
        self.additionalparam5 = additionalparam5  # type: str
        self.additionalparam6 = additionalparam6  # type: str
        self.additionalparam7 = additionalparam7  # type: str
        self.additionalparam8 = additionalparam8  # type: str
        self.additionalparam9 = additionalparam9  # type: str
        self.additionalparam10 = additionalparam10  # type: str
        
    def __repr__(self):
        return repr(self.__dict__)

    @classmethod
    def from_dict(cls, AdditionalParam_dict: Dict[str, Union[str, str, str,str,str,str,str,str,str]]) -> 'AdditionalParam':
        return cls(additionalparam1=AdditionalParam_dict['additionalparam1'],
                   additionalparam2=AdditionalParam_dict['additionalparam2'],
                   additionalparam3=AdditionalParam_dict['additionalparam3'],
                   additionalparam4=AdditionalParam_dict['additionalparam4'],
                   additionalparam5=AdditionalParam_dict['additionalparam5'],
                   additionalparam6=AdditionalParam_dict['additionalparam6'],
                   additionalparam7=AdditionalParam_dict['additionalparam7'],
                   additionalparam8=AdditionalParam_dict['additionalparam8'],
                   additionalparam9=AdditionalParam_dict['additionalparam9'],
                   additionalparam10=AdditionalParam_dict['additionalparam10']
                   )

class PipelineConfiguration:
    def __init__(self,
                 file_format: DataFileFormat,
                 temp_bigquery_table: BigQueryTable,
                 main_bigquery_table: BigQueryTable,
                 sql_operations: SqlOperation,
                 source_gcs_folder: GcsFolder,
                 process_gcs_folder: GcsFolder,
                 backup_gcs_folder: GcsFolder,
                 duplicate_gcs_folder: GcsFolder,
                 error_gcs_folder: GcsFolder,
                 archive_gcs_folder: GcsFolder,
                 sp_operations: SpOperation,
                 sts_operations: STSOperation,
                 additionalparam: AdditionalParam
                 ) -> None:

        self.file_format = file_format
        self.temp_bigquery_table = temp_bigquery_table
        self.main_bigquery_table = main_bigquery_table
        self.sql_operations = sql_operations    # type: SqlOperation
        self.source_gcs_folder = source_gcs_folder  # type: GcsFolder
        self.process_gcs_folder = process_gcs_folder  # type: GcsFolder
        self.backup_gcs_folder = backup_gcs_folder  # type: GcsFolder
        self.duplicate_gcs_folder = duplicate_gcs_folder  # type: GcsFolder
        self.error_gcs_folder = error_gcs_folder  # type: GcsFolder
        self.archive_gcs_folder = archive_gcs_folder  # type: GcsFolder
        self.sp_operations = sp_operations  # type: SpOperation
        self.sts_operations = sts_operations  # type: STSOperation
        self.additionalparam = additionalparam # type: AdditionalParam

    def __repr__(self):
        return repr(self.__dict__)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> 'PipelineConfiguration':
        return cls(file_format=DataFileFormat.from_dict(config_dict['file_format']),
                   temp_bigquery_table=BigQueryTable.from_dict(config_dict['temp_bigquery_table']),
                   main_bigquery_table=BigQueryTable.from_dict(config_dict['main_bigquery_table']),
                   source_gcs_folder=GcsFolder.from_dict(config_dict['source_gcs_folder']),
                   process_gcs_folder=GcsFolder.from_dict(config_dict['process_gcs_folder']),
                   backup_gcs_folder=GcsFolder.from_dict(config_dict['backup_gcs_folder']),
                   duplicate_gcs_folder=GcsFolder.from_dict(config_dict['duplicate_gcs_folder']),
                   error_gcs_folder=GcsFolder.from_dict(config_dict['error_gcs_folder']),
                   archive_gcs_folder=GcsFolder.from_dict(config_dict['archive_gcs_folder']),
                   sp_operations=SpOperation.from_dict(config_dict['sp_operations']),
                   sts_operations=STSOperation.from_dict(config_dict['sts_operations']),
                   sql_operations=SqlOperation.from_dict(config_dict['sql_operations']),
                   additionalparam=AdditionalParam.from_dict(config_dict['additionalparam'])
                   )