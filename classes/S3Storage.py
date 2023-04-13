import os
import boto3

from classes.FileMetadata import FileMetadata

class S3Storage:

    s3 = None

    def __init__(self) -> None:
        self.s3 = boto3.resource('s3', endpoint_url=os.getenv('AWS_S3_ENDPOINT'))

    def upload_file(self, file_path: str, file_metadata: FileMetadata) -> None:
        """
        Uploads file into S3 storage.

        Parameters:
        file_path (string): Absolute path to file where it will be stored in S3. (eg. disk/projects/hello.txt)
        """

        with open(("temp/" + file_path).encode('utf-8'), 'rb') as file:
            obj = self.s3.Object(
                os.getenv('AWS_BUCKET'),
                file_path + str(file_metadata.modified_time)
            )
            obj.put(Body=file, StorageClass="GLACIER")

    def initiate_download(self, file_path: str, modified_time: int) -> None:
        """
        """

        # return # TODO Remove in production

        obj = self.s3.Object(
            os.getenv('AWS_BUCKET'),
            file_path + str(modified_time)
        )

        if obj.restore is None:
            print('Submitting restoration request: %s' % obj.key)
            obj.restore_object(RestoreRequest={'Days': 1, 'GlacierJobParameters': {'Tier': 'Bulk'}})
            # Print out objects whose restoration is on-going
        elif 'ongoing-request="true"' in obj.restore:
            print('Restoration in-progress: %s' % obj.key)
            # Print out objects whose restoration is complete
        elif 'ongoing-request="false"' in obj.restore:
            print('Restoration complete: %s' % obj.key )