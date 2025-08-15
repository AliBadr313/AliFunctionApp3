using System;
using System.IO;
using System.Threading.Tasks;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Renci.SshNet; // For SftpClient


namespace AUFS_LIVE;

public class LIVE_AUFS_AUSWIDE_SUBWAY_MoveBlobsToSftpTimer
{
     private readonly ILogger<LIVE_AUFS_AUSWIDE_SUBWAY_MoveBlobsToSftpTimer> _logger;
        private readonly string sftpHost;
        private readonly int sftpPort;
        private readonly string sftpUser;
        private readonly string sftpPassword;
        private readonly string sftpBasePath;
        private readonly string storageConnectionString;

        public LIVE_AUFS_AUSWIDE_SUBWAY_MoveBlobsToSftpTimer(ILogger<LIVE_AUFS_AUSWIDE_SUBWAY_MoveBlobsToSftpTimer> logger)
        {
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));

            storageConnectionString = Environment.GetEnvironmentVariable("BLOB_CONNECTION_STRING")
                ?? throw new InvalidOperationException("BLOB_CONNECTION_STRING not set");

            sftpHost = Environment.GetEnvironmentVariable("SFTP_HOST")
                ?? throw new InvalidOperationException("SFTP_HOST not set");

            sftpPort = int.Parse(Environment.GetEnvironmentVariable("SFTP_PORT") ?? "22");

            sftpUser = Environment.GetEnvironmentVariable("SFTP_USER")
                ?? throw new InvalidOperationException("SFTP_USER not set");

            sftpPassword = Environment.GetEnvironmentVariable("SFTP_PASSWORD")
                ?? throw new InvalidOperationException("SFTP_PASSWORD not set");

            sftpBasePath = Environment.GetEnvironmentVariable("LIVE_SFTP_BASE_PATH") ?? "/Inbound/";
        }

        [Function("LIVE_AUFS_AUSWIDE_SUBWAY_MoveBlobsToSftpTimer")]
        public async Task RunAsync([TimerTrigger("0 * * * * *")] TimerInfo myTimer)
        {
            _logger.LogInformation("Timer function executed at: {time}", DateTime.UtcNow);

            var blobServiceClient = new BlobServiceClient(storageConnectionString);
            var containerClient = blobServiceClient.GetBlobContainerClient("exportcontainer-live");

            await foreach (var blobItem in containerClient.GetBlobsAsync())
            {
                string name = blobItem.Name;
                _logger.LogInformation("Processing blob: {blobName}", name);

                string? targetFolder = GetTargetFolder(name);
                if (targetFolder is null)
                {
                    _logger.LogWarning("Skipping {blobName} — no matching folder.", name);
                    continue;
                }

                try
                {
                    // Download blob
                    var blobClient = containerClient.GetBlobClient(name);
                    await using var blobStream = new MemoryStream();
                    await blobClient.DownloadToAsync(blobStream);
                    blobStream.Position = 0;

                    // Upload to SFTP
                    using (var sftp = new SftpClient(sftpHost, sftpPort, sftpUser, sftpPassword))
                    {
                        sftp.Connect();

                        string sftpFolderPath = $"{sftpBasePath}/{targetFolder}";
                        if (!sftp.Exists(sftpFolderPath))
                        {
                            sftp.CreateDirectory(sftpFolderPath);
                        }

                        sftp.UploadFile(blobStream, $"{sftpFolderPath}/{name}");
                        sftp.Disconnect();
                    }

                

                  // Read destination container name from environment variable
                    var destinationContainerName = Environment.GetEnvironmentVariable("DESTINATION_CONTAINER_NAME_LIVE");
                    if (string.IsNullOrWhiteSpace(destinationContainerName))
                    {
                        throw new Exception("Environment variable DESTINATION_CONTAINER_NAME is not set.");
                    }

                    // Create client for destination container
                    var destinationContainerClient = new BlobContainerClient(
                        storageConnectionString,
                        destinationContainerName
                    );

                    // Get blob name only (no virtual folders unless you want them preserved)
                    var destinationBlobName = Path.GetFileName(blobClient.Name);
                    var destinationBlobClient = destinationContainerClient.GetBlobClient(destinationBlobName);

                    // Copy blob to destination container
                    await destinationBlobClient.StartCopyFromUriAsync(blobClient.Uri);

                    // Wait until copy completes
                    BlobProperties destProps;
                    do
                    {
                        await Task.Delay(500); // 0.5 second between checks
                        destProps = await destinationBlobClient.GetPropertiesAsync();
                    }
                    while (destProps.CopyStatus == CopyStatus.Pending);

                    // Delete original if copy succeeded
                    if (destProps.CopyStatus == CopyStatus.Success)
                    {
                        await blobClient.DeleteIfExistsAsync();
                    }
                    else
                    {
                        throw new Exception($"Failed to copy blob '{blobClient.Name}' to '{destinationContainerName}'. Copy status: {destProps.CopyStatus}");
                    }


                }
                catch (Exception ex)
                {
                    _logger.LogError(ex, "Failed to process blob {blobName}", name);

                    // Log more info explicitly
                    _logger.LogError("Exception type: {type}", ex.GetType().FullName);
                    _logger.LogError("Message: {msg}", ex.Message);
                    _logger.LogError("StackTrace: {stack}", ex.StackTrace);
                }

            }
        }

        private static string? GetTargetFolder(string fileName)
        {
            if (fileName.StartsWith("Invoice_", StringComparison.OrdinalIgnoreCase)) return "Invoices";
            if (fileName.StartsWith("Credit_", StringComparison.OrdinalIgnoreCase)) return "Credits";
            if (fileName.StartsWith("Stock_", StringComparison.OrdinalIgnoreCase)) return "Stock";
            return null;
        }

}