from datetime import datetime
import os
import aiohttp
from azure.storage.blob import BlobServiceClient, BlobProperties, generate_blob_sas, BlobSasPermissions
from time import perf_counter
from asyncio import run

async def main():
    conn_string = os.getenv('STORAGE_CONNECTION_STRING')
    if conn_string is None:
        raise TypeError("Env var STORAGE_CONNECTION_STRING not set")

    account_name = os.getenv('AZURE_STORAGE_ACCOUNT_NAME')
    if account_name is None:
        raise TypeError("Env var AZURE_STORAGE_ACCOUNT_NAME not set")

    account_key = os.getenv('AZURE_STORAGE_ACCOUNT_KEY')
    if conn_string is None:
        raise TypeError("Env var AZURE_STORAGE_ACCOUNT_KEY not set")

    service_client: BlobServiceClient = BlobServiceClient.from_connection_string(conn_string)

    container_name = 'samples'
    container_client = service_client.get_container_client(container_name)

    blobs_list: list[BlobProperties] = sorted(container_client.list_blobs(), key=lambda b: b.size)
    for blob in blobs_list:
        blob_sas = generate_blob_sas(account_name, container_name, blob.name, account_key=account_key, permission=BlobSasPermissions(read=True), expiry=datetime(2023, 1, 1))
        blob_url = f'https://{account_name}.blob.core.windows.net/{container_name}/{blob.name}?{blob_sas}'
        async with aiohttp.ClientSession() as session:
            print(f'Downloading {blob.size} bytes from blob {blob.name}...')
            e1 = None
            p1 = perf_counter()
            async with session.get(blob_url) as response:
                c1 = await response.read()
                e1 = (perf_counter() - p1) * 1000
                print(f'Downloaded {len(c1)} bytes in {e1} ms')

            
            print(f'Downloading {blob.size} bytes from blob {blob.name}...')
            e2 = None
            p2 = perf_counter()
            async with session.get(blob_url) as response:
                c2 = await response.read()
                e2 = (perf_counter() - p2) * 1000
                print(f'Downloaded {len(c2)} bytes in {e2} ms')

            print(f'Cold overhead: {e1 - e2}')
            print()

run(main())