import minio
import logging


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
)

logger = logging.getLogger(__name__)


class MinioClient:
    def __init__(self):
        self.client = minio.Minio(endpoint='localhost:9000',
                                  access_key='minioadmin',
                                  secret_key='minioadmin',
                                  secure=False)

    def make_bucket(self, bucket_name):
        try:
            if not self.client.bucket_exists(bucket_name):
                self.client.make_bucket(bucket_name)
                return (f"{bucket_name} 创建成功!")
            return (f"{bucket_name} 已存在!")
        except Exception as e:
            logger.error(f"Failed to make bucket: {bucket_name}! \n{e}")
            return ("创建失败！请按规范填写名称或查看MinIO连接是否正常。")
    
    def remove_bucket(self, bucket_name):
        try:
            if self.client.bucket_exists(bucket_name):
                self.client.remove_bucket(bucket_name)
                return (f"{bucket_name} 已删除！")
            return (f"{bucket_name} 不存在！")
        except Exception as e:
            logger.error(f"Failed to remove bucket: {bucket_name}! \n{e}")
            return ("删除失败！请先删除Bucket内所有文件。")
    
    def list_buckets(self):
        _ = self.client.list_buckets()
        bucket_name_list = []
        for bucket in _:
            bucket_name_list.append(bucket.name)
        return bucket_name_list
    
    def list_objects(self, bucket_name):
        objs = []
        if self.client.bucket_exists(bucket_name):
            _ = self.client.list_objects(bucket_name, recursive=True)
            for obj in _:
                objs.append(obj.object_name)
        return objs
    
    def put_object(self, bucket_name, object_name, data):
        # Uploads data from a stream to an object in a bucket.
        try:
            result = self.client.put_object(
                bucket_name, object_name, data, 
                length=-1, part_size=5*1024*1024, 
            )
            return (f"上传: {result.object_name} 成功！")
        except Exception as e:
            logger.error(f"Failed to put object: {object_name}! \n{e}")
            return (f"上传 {object_name} 失败！")
    
    def get_object(self, bucket_name, object_name):
        # Gets data from offset to length of an object.
        try:
            response = self.client.get_object(bucket_name, object_name)
            data = response.data
            return data
        except Exception as e:
            logger.error(f"Failed to get object: {object_name}! \n{e}")
            return (f"下载 {object_name} 失败！")
        finally:
            response.close()
            response.release_conn()
    
    def fget_object(self, bucket_name, object_name, file_path):
        # Downloads data of an object to file.
        try:
            self.client.fget_object(bucket_name, object_name, file_path)
        except Exception as e:
            logger.error(f"Failed to fget object: {object_name}! \n{e}")
            return (f"缓存 {object_name} 失败！")
        
    def remove_object(self, bucket_name, object_name):
        # Remove an object.
        try:
            self.client.remove_object(bucket_name, object_name)
            return (f"{object_name} 已删除！")
        except Exception as e:
            logger.error(f"Failed to remove object: {object_name}! \n{e}")
            return (f"删除 {object_name} 失败！")

