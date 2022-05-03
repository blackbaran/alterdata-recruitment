from PrepareData import PrepareData


def create_new_data(
    project_name,
    bucket_name,
    ds_name,
    table_name,
    file_name,
    localization = "US"):

    """
    Description
    ----------
    Function to check if data is already on GCP bucket, if yes - import data table into BQ
    Parameters
    ----------
    project_name: str
        name of project on GCP,
    bucket_name: str
        name of bucket where function should check if data exist
    ds_name: str
        name of data set on BQ - if doesn' exist will create a new one
    table_name: str
        name of the BQ table to which the data will be downloaded
    file_name: str
        name of the file to be transferred to BQ
    localization: str
        default US
    """
    
    client = PrepareData(
        project_id=project_name,
        bucket_name=bucket_name,
        ds_name=ds_name,
        table_name=table_name,
        file_name=file_name,
        localization=localization)

    exist_data = client.check_exist_data()

    if exist_data:
        client.check_exits_data_set()
        client.create_data_table()
    else:
        print("Data dosen't exist!")






