from obs import ObsClient
from obs import CreateBucketHeader
import traceback

class CloudBucket:
    def __init__(self, access_key_id: str, secret_access_key: str, server_location: str) -> None:
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self.server_location = server_location

    def start_obs_connection(self):
        self.obsClient = ObsClient(
            access_key_id=self.access_key_id,
            secret_access_key=self.secret_access_key,
            server=self.server_location
        )

    def check_connection(self) -> None:
        if self.obsClient is None:
            raise Exception("The OBS connection has was not started")

    def close_obs_connection(self):
        if self.obsClient is not None:
            self.obsClient.close()
            self.obsClient = None

    def __enter__(self):
        self.start_obs_connection()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close_obs_connection()

    def list_obs_buckets(self) -> dict:
        self.check_connection()
        try:
            response = self.obsClient.listBuckets(isQueryLocation=True) 
            return response
        except Exception as error:
            traceback.print_exc
    
    def bucket_already_exists(self, bucketName: str) -> dict:
        self.check_connection()
        try:
            response = self.obsClient.headBucket(bucketName=bucketName)
            bucket_exists = True if response.status < 300 else False


            return {
                'bucket_exists': bucket_exists,
                'response': response
                }
        except Exception as error:
            traceback.print_exc
    
    def create_obs_bucket(self, bucketName: str, region: str) -> dict:
        self.check_connection()
        try:
            response = self.obsClient.createBucket(bucketName=bucketName, location=region)
            return response
        except Exception as error:
            traceback.print_exc

    def get_binary_file(self, bucketName: str, file_name: str) -> dict:
        self.check_connection()
        try:
            response = self.obsClient.getObject(bucketName=bucketName, objectKey=file_name, loadStreamInMemory=True)
            file_exists = True if response.status < 300 else False
            if file_exists: 
                return response
            return {"status": "error while downloading the file"} 
                
        except:
            import traceback
            print(traceback.format_exc())

if __name__ == "__main__":
    from decouple import config
    import pandas as pd
    import io


    server_location = 'obs.sa-brazil-1.myhuaweicloud.com'
    access_key_id = config('ACCESS_KEY_ID')
    secret_access_key = config('SECRET_ACCESS_KEY')
    with CloudBucket(access_key_id=access_key_id, secret_access_key=secret_access_key, server_location=server_location) as client:
        response = client.get_binary_file(bucketName='test-datalake', file_name='uber_data.csv')
        print(response.keys())
        df = pd.read_csv(io.BytesIO(response.body.buffer), sep=',')
        print(df)

