from google.cloud import bigquery
from google.cloud.exceptions import NotFound
from google.cloud import storage

class PrepareData():

    def __init__(
        self, 
        project_id,
        ds_name,
        bucket_name,
        table_name,
        file_name,
        localization
        ):

        self.project_id = project_id
        self.ds_name = ds_name
        self.bucket_name = bucket_name
        self.table_name = table_name
        self.file_name = file_name
        self.localization = localization
        self.client = bigquery.Client(project_id)
        self.storage_client = storage.Client(project_id)

    def create_data_set(self):
        """
        Description
        ----------
        Function to create new data set on BQ view
        """

        dataset_id = "{}.{}".format(self.client.project, self.ds_name)
        dataset = bigquery.Dataset(dataset_id)
        dataset.location = self.localization

        dataset = self.client.create_dataset(dataset, timeout=30)  # Make an API request.
        print("Created dataset {}.{}".format(self.client.project, dataset.dataset_id))

    def check_exits_data_set(self):
        """
        Description
        ----------
        Function to check if data set already exist, if none - check the new one
        """

        try:
            self.client.get_dataset(self.ds_name)  
            print("Dataset {} already exists".format(self.ds_name))
        except NotFound:
            self.create_data_set()
            print("Dataset {} was created".format(self.ds_name))

    def create_data_table(self):
        """
        Description
        ----------
        Function to create new data table in given data set in BQ
        """

        table_id = '{}.{}.{}'.format(self.project_id, self.ds_name, self.table_name)

        job_config = bigquery.LoadJobConfig(
            autodetect=True, 
            skip_leading_rows=1,
            source_format=bigquery.SourceFormat.CSV)

        uri = 'gs://{}/{}'.format(self.bucket_name, self.file_name)

        load_job = self.client.load_table_from_uri(
            uri, table_id, job_config=job_config) 
        
        load_job.result()  # Waits for the job to complete.
        destination_table = self.client.get_table(table_id)
        print("Loaded {} rows.".format(destination_table.num_rows))

    def check_exist_data(self):
        """
        Description
        ----------
        Function to chech if data set exist in bucket in GCP
        """

        client = self.storage_client
        bucket = client.bucket(self.bucket_name)
        blob = bucket.get_blob(self.file_name)
        try:
            return blob.exists(client)
        except:
            return False







