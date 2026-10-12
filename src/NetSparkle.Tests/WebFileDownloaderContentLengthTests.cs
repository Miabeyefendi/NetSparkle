using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Threading.Tasks;
using NetSparkleUpdater.Downloaders;
using NetSparkleUpdater.Events;
using Xunit;

namespace NetSparkleUnitTests
{
    public class WebFileDownloaderContentLengthTests
    {
        private const int ChunkSize = 32 * 1024;
        private const int ChunkCount = 10;

        private static Uri GetFreeUrl()
        {
            var tcpListener = new TcpListener(IPAddress.Loopback, 0);
            tcpListener.Start();
            int port = ((IPEndPoint)tcpListener.LocalEndpoint).Port;
            tcpListener.Stop();
            return new Uri("http://127.0.0.1:" + port + "/file.bin");
        }

        private static HttpListener StartServer(Uri url, Func<HttpListenerContext, Task> handler)
        {
            var listener = new HttpListener();
            listener.Prefixes.Add(url.GetLeftPart(UriPartial.Authority) + "/");
            listener.Start();
            Task.Run(async () =>
            {
                try
                {
                    var context = await listener.GetContextAsync();
                    await handler(context);
                }
                catch
                {
                    // client went away or the listener was closed; nothing to do
                }
            });
            return listener;
        }

        /// <summary>
        /// Send the body in several chunks and without a Content-Length header
        /// </summary>
        private static async Task SendChunkedBody(HttpListenerContext context)
        {
            context.Response.SendChunked = true;
            for (int i = 0; i < ChunkCount; i++)
            {
                var chunk = Enumerable.Repeat((byte)(i + 1), ChunkSize).ToArray();
                await context.Response.OutputStream.WriteAsync(chunk, 0, chunk.Length);
                await context.Response.OutputStream.FlushAsync();
                await Task.Delay(10);
            }
            context.Response.Close();
        }

        private class DownloadResult
        {
            public List<ItemDownloadProgressEventArgs> Progress = new List<ItemDownloadProgressEventArgs>();
            public List<AsyncCompletedEventArgs> Completed = new List<AsyncCompletedEventArgs>();
            public byte[] File = new byte[0];
        }

        private static async Task<DownloadResult> Download(Uri url, long expectedDownloadSize)
        {
            var path = Path.Combine(Path.GetTempPath(), "netsparkle-test-" + Guid.NewGuid().ToString("N") + ".bin");
            var result = new DownloadResult();
            try
            {
                using (var downloader = new WebFileDownloader())
                {
                    downloader.ExpectedDownloadSize = expectedDownloadSize;
                    downloader.PrepareToDownloadFile();
                    downloader.DownloadProgressChanged += (sender, e) => result.Progress.Add(e);
                    downloader.DownloadFileCompleted += (sender, e) => result.Completed.Add(e);
                    await downloader.DownloadFile(url, path);
                }
                if (File.Exists(path))
                {
                    result.File = File.ReadAllBytes(path);
                }
            }
            finally
            {
                if (File.Exists(path))
                {
                    File.Delete(path);
                }
            }
            return result;
        }

        [Fact]
        public async Task DownloadWithoutContentLengthHeaderSucceeds()
        {
            var url = GetFreeUrl();
            var listener = StartServer(url, SendChunkedBody);
            try
            {
                var result = await Download(url, 0);

                var completed = Assert.Single(result.Completed);
                Assert.Null(completed.Error);
                Assert.False(completed.Cancelled);
                Assert.Equal(ChunkSize * ChunkCount, result.File.Length);
                Assert.Equal((byte)ChunkCount, result.File[result.File.Length - 1]);
                // the size is not known while downloading, so the percentage can't be either...
                foreach (var progress in result.Progress.Take(result.Progress.Count - 1))
                {
                    Assert.Equal(0, progress.ProgressPercentage);
                    Assert.Equal(0, progress.TotalBytesToReceive);
                }
                // ...but when the download is done, it is 100%
                var last = result.Progress.Last();
                Assert.Equal(100, last.ProgressPercentage);
                Assert.Equal(ChunkSize * ChunkCount, last.BytesReceived);
                Assert.Equal(ChunkSize * ChunkCount, last.TotalBytesToReceive);
            }
            finally
            {
                listener.Abort();
            }
        }

        [Theory]
        [InlineData(ChunkSize * ChunkCount * 2)] // expected size is too big
        [InlineData(ChunkSize * 3)]              // expected size is too small
        public async Task ExpectedDownloadSizeIsUsedForProgressWithoutContentLengthHeader(long expectedDownloadSize)
        {
            var url = GetFreeUrl();
            var listener = StartServer(url, SendChunkedBody);
            try
            {
                var result = await Download(url, expectedDownloadSize);

                Assert.Null(Assert.Single(result.Completed).Error);
                Assert.Equal(ChunkSize * ChunkCount, result.File.Length);
                var inProgress = result.Progress.Take(result.Progress.Count - 1).ToList();
                Assert.Contains(inProgress, p => p.ProgressPercentage > 0);
                int previousPercentage = 0;
                foreach (var progress in inProgress)
                {
                    // an estimate must never claim the download is finished or bigger than what we received
                    Assert.InRange(progress.ProgressPercentage, previousPercentage, 99);
                    Assert.True(progress.TotalBytesToReceive >= progress.BytesReceived);
                    previousPercentage = progress.ProgressPercentage;
                }
                var last = result.Progress.Last();
                Assert.Equal(100, last.ProgressPercentage);
                Assert.Equal(ChunkSize * ChunkCount, last.TotalBytesToReceive);
            }
            finally
            {
                listener.Abort();
            }
        }

        [Fact]
        public async Task ContentLengthHeaderIsUsedInsteadOfExpectedDownloadSize()
        {
            var url = GetFreeUrl();
            var body = new byte[100 * 1024];
            var listener = StartServer(url, async context =>
            {
                context.Response.ContentLength64 = body.Length;
                await context.Response.OutputStream.WriteAsync(body, 0, body.Length);
                context.Response.Close();
            });
            try
            {
                var result = await Download(url, 999);

                Assert.Null(Assert.Single(result.Completed).Error);
                Assert.Equal(body.Length, result.File.Length);
                Assert.All(result.Progress, p => Assert.Equal(body.Length, p.TotalBytesToReceive));
                Assert.Equal(100, result.Progress.Last().ProgressPercentage);
            }
            finally
            {
                listener.Abort();
            }
        }

        [Fact]
        public async Task ErrorStatusCodeIsStillAnErrorWithoutContentLengthHeader()
        {
            var url = GetFreeUrl();
            var listener = StartServer(url, context =>
            {
                context.Response.StatusCode = 500;
                context.Response.SendChunked = true;
                context.Response.Close();
                return Task.CompletedTask;
            });
            try
            {
                var result = await Download(url, 0);

                var completed = Assert.Single(result.Completed);
                Assert.NotNull(completed.Error);
                Assert.Contains("InternalServerError", completed.Error.Message);
                Assert.Empty(result.File);
            }
            finally
            {
                listener.Abort();
            }
        }
    }
}
