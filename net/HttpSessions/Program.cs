using Azure.Storage.Blobs;
using Azure.Storage.Sas;
using System.Diagnostics;

namespace HttpSessions
{
    internal class Program
    {
        static async Task Main(string[] args)
        {
            var connString = Environment.GetEnvironmentVariable("STORAGE_CONNECTION_STRING") ??
                throw new InvalidOperationException("Environment variable STORAGE_CONNECTION_STRING not set");

            var containerClient = new BlobContainerClient(connString, "samples");

            var blobs = containerClient.GetBlobs().OrderBy(b => b.Properties.ContentLength);

            foreach (var blob in blobs)
            {
                var blobClient = containerClient.GetBlobClient(blob.Name);
                var sasUri = blobClient.GenerateSasUri(BlobSasPermissions.Read, DateTimeOffset.Now.AddYears(1));
                using (var client = new HttpClient())
                {
                    var cold = await Download(client, blob.Name, blob.Properties.ContentLength, sasUri);
                    var warm = await Download(client, blob.Name, blob.Properties.ContentLength, sasUri);
                    Console.WriteLine($"Cold overhead: {cold - warm}");
                }
            }
        }

        private static async Task<TimeSpan> Download(HttpClient client, string name, long? size, Uri sasUri)
        {
            Console.WriteLine($"Downloading {size} bytes from blob '{name}'...");
            using (var countingStream = new CountingStream())
            {
                var sw = Stopwatch.StartNew();
                using (var stream = await client.GetStreamAsync(sasUri))
                {
                    await stream.CopyToAsync(countingStream);
                }
                sw.Stop();
                Console.WriteLine($"Downloaded {countingStream.Length} bytes in {sw.Elapsed}");
                return sw.Elapsed;
            }
        }

        private class CountingStream : Stream
        {
            private long _count;

            public override bool CanRead => false;

            public override bool CanSeek => false;

            public override bool CanWrite => true;

            public override long Length => _count;

            public override long Position { get => throw new NotImplementedException(); set => throw new NotImplementedException(); }

            public override void Flush()
            {
            }

            public override int Read(byte[] buffer, int offset, int count)
            {
                throw new NotImplementedException();
            }

            public override long Seek(long offset, SeekOrigin origin)
            {
                throw new NotImplementedException();
            }

            public override void SetLength(long value)
            {
                throw new NotImplementedException();
            }

            public override void Write(byte[] buffer, int offset, int count)
            {
                _count += count;
            }
        }
    }
}