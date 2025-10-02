from google.cloud import storage

client = storage.Client()
print([b.name for b in client.list_buckets()])