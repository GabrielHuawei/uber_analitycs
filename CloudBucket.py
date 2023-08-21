from obs import ObsClient
from obs import CreateBucketHeader
import traceback
from typing import Optional, Union, Dict, Callable


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


            # (self, bucketName, objectKey, uploadFile, partSize=9 * 1024 * 1024,
            #        taskNum=1, enableCheckpoint=False, checkpointFile=None,
            #        checkSum=False, metadata=None, progressCallback=None, headers=None,
            #        extensionHeaders=None, encoding_type=None):

    def upload_file(self,
                    bucketName: str,
                    objectKey: str,
                    uploadFile: Union[str, bytes],
                    partSize: Optional[int] = 9 * 1024 * 1024,
                    taskNum: Optional[int] = 1,
                    enableCheckpoint: Optional[bool] = False,
                    checkpointFile: Optional[str] = None,
                    checkSum: Optional[bool] = False,
                    metadata: Optional[Dict] = None,
                    progressCallback: Optional[Callable] = None,
                ) -> dict:
        try:
            response = self.obsClient.uploadFile(bucketName, objectKey, uploadFile, partSize)
            print(response)
            if response.status < 300:
                print('requestId:', response.requestId)
            else:
                print('errorCode:', response.errorCode)
                print('errorMessage:', response.errorMessage)

            return response
        except:
            print(traceback.format_exc())



if __name__ == "__main__":
    from decouple import config
    import pandas as pd
    import io


    server_location = 'obs.sa-brazil-1.myhuaweicloud.com'
    access_key_id ='UUPFWV82Q3J8VYVHNZBQ'
    secret_access_key = 'Ib4bDYbx5jdLrjDQulpiQRSKGPer5ffkx5ppi9ug' 
    with CloudBucket(access_key_id=access_key_id, secret_access_key=secret_access_key, server_location=server_location) as client:
        df = pd.read_csv(r'assets\uber_data.csv')
        csv_bytes = df.to_csv(index=False).encode()
        print(csv_bytes[:100]) 

        bucket_name = 'test-datalake'
        object_key = 'uploadUberData.csv'
        part_size = 10 * 1024 * 1024
        file_path = r"C:\Users\g50034179\Documents\studies\uber_analytics\assets\uber_data.csv"

        try:
            resp = client.upload_file(bucket_name, object_key, file_path)
            print(resp)
            if resp.status < 300:
                print('requestId:', resp.requestId)
            else:
                print('errorCode:', resp.errorCode)
                print('errorMessage:', resp.errorMessage)
        except:
            import traceback
            print(traceback.format_exc())

        # response = client.get_binary_file(bucketName='test-datalake', file_name='uber_data.csv')

        # print(response)
        # # df = pd.read_csv(io.BytesIO(response.body.buffer), sep=',')
        # # print(df)

