# Storage Continuous Data Protection (SCDP)

[Continuous Data Protection](https://en.wikipedia.org/wiki/Continuous_Data_Protection) is a backup system which captures changed files and archives them. Therefore, it is possible to restore data to any point in time.

Currently there is 1 source available:
* Google Drive

All of these sources are stored in [Amazon S3 API](https://docs.aws.amazon.com/AmazonS3/latest/API/Welcome.html) compatible object storage. Archived data is stored in Glacier storage so that it is price efficient.