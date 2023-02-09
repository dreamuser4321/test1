import logging
import boto3
from botocore.exceptions import ClientError


class S3Connect:
    def __init__(self, file_name, bucket, object_name=None, run_env="SERVER"):
        """
        Initialize S3 connect through boto3

        :param file_name: File to upload
        :param bucket: Bucket to upload to
        :param object_name: S3 object name. If not specified then file_name is used
        """
        self.run_env = run_env
        self.file_name = file_name
        self.bucket = bucket
        self.object_name = object_name

    def count_files_in_folder(self, folder, run_env):
        print("count_files_in_folder - bucket: {}. Folder: {}".format(self.bucket, folder))
        if run_env == "SERVER":
            print("Boto3 - loading default config")
            boto3.setup_default_session()
        else:
            print("Boto3 - loading saml config")
            boto3.setup_default_session(profile_name='saml')
        s3 = boto3.client('s3')
        response = s3.list_objects(
            Bucket=self.bucket,
            Prefix=folder,
        )
        print("count_files_in_folder - Response: {}".format(str(response)))
        return len(response['Contents'])

    def upload_file(self, run_env):
        """Upload a file to an S3 bucket

        :return: True if file was uploaded, else False
        """

        # If S3 object_name was not specified, use file_name
        if self.object_name is None:
            self.object_name = self.file_name

        if run_env == "SERVER":
            print("Boto3 - loading default config")
            boto3.setup_default_session()
        else:
            print("Boto3 - loading saml config")
            boto3.setup_default_session(profile_name='saml')

        # Upload the file
        s3_client = boto3.client('s3')
        try:
            print("upload_file(). file_name: {}. bucket: {}. object_name: {}".format(self.file_name, self.bucket, self.object_name))
            response = s3_client.upload_file(self.file_name, self.bucket, self.object_name)
        except ClientError as e:
            logging.error(e)
            return False
        return True
