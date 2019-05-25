from google.cloud import storage

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """Uploads a file to the bucket."""
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(destination_blob_name)

    blob.upload_from_filename(source_file_name)

    print('File {} uploaded to {}.'.format(
        source_file_name,
        destination_blob_name))

bucket_name = 'pickle-file'
source_file_name_1 = 'G_adj.p'
source_file_name_2 = 'G_edges_proj.p'

upload_blob(bucket_name, source_file_name_1, destination_blob_name)
upload_blob(bucket_name, source_file_name_2, destination_blob_name)








