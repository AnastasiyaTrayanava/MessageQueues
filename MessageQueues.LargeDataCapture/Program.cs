using MessageQueues.Common.Models;
using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.IO;
using System.Runtime.Serialization.Formatters.Binary;

namespace MessageQueues.LargeDataCapture
{
    internal class Program
    {
        private const string _listeningFolder = @"C:\Inbox";
        private const string _fileFormats = "*.txt";
        private const string _queueName = "largequeue";
        private const int _chunkSize = 1048576;

        private static IModel _channel;
        private static FileSystemWatcher _watcher;

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory { HostName = "localhost" };
            using var connection = factory.CreateConnection();
            _channel = connection.CreateModel();

            _channel.QueueDeclare(queue: _queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            SetupFileWatcher();

            Console.ReadLine();
            connection.Close();
        }

        private static void SetupFileWatcher()
        {
            _watcher = new FileSystemWatcher(_listeningFolder);

            _watcher.NotifyFilter = NotifyFilters.FileName;

            _watcher.Created += OnCreated;
            _watcher.Changed += OnChanged;

            _watcher.Filter = _fileFormats;

            _watcher.EnableRaisingEvents = true;

            Console.WriteLine($"Listening to folder {_listeningFolder}");
        }

        private static void OnChanged(object sender, FileSystemEventArgs e)
        {
            if (e.ChangeType != WatcherChangeTypes.Changed)
            {
                return;
            }
            Console.WriteLine($"Changed: {e.FullPath}");

            var file = new FileInfo(e.FullPath);
            while (IsFileLocked(file))
            {
                Console.WriteLine($"File is locked: {e.FullPath}");
            }
            SendMessage(e.FullPath);
        }

        private static void OnCreated(object sender, FileSystemEventArgs e)
        {
            Console.WriteLine($"Created: {e.FullPath}");
            var file = new FileInfo(e.FullPath);
            while (IsFileLocked(file))
            {
                Console.WriteLine($"File is locked: {e.FullPath}");
            }
            SendMessage(e.FullPath);
        }

        private static void SendMessage(string filePath)
        {
            var count = 0;

            var fileStream = File.OpenRead(filePath);

            var fileSize = fileStream.Length;
            var remainingFileSize = Convert.ToInt32(fileStream.Length);
            var chunksAmount = Convert.ToInt32(Math.Ceiling(decimal.Divide(fileSize, _chunkSize)));

            while (remainingFileSize > 0)
            {
                int read;
                byte[] buffer;
                count++;

                if (remainingFileSize > _chunkSize)
                {
                    buffer = new byte[_chunkSize];
                    read = fileStream.Read(buffer, 0, _chunkSize);
                }
                else
                {
                    buffer = new byte[remainingFileSize];
                    read = fileStream.Read(buffer, 0, remainingFileSize);
                }

                var messageBody = new FileProperties
                {
                    Name = Path.GetFileName(filePath),
                    Body = buffer,
                    ChunkNumber = count,
                    TotalChunks = chunksAmount
                };

                var message = SerializeMessage(messageBody);

                var basicProperties = _channel.CreateBasicProperties();
                basicProperties.Persistent = true;

                _channel.BasicPublish(
                    string.Empty, 
                    _queueName, 
                    basicProperties,
                    message);

                Console.WriteLine($"Chunk #{count}/{chunksAmount} sent: {filePath}");
                remainingFileSize -= read;
            }

            fileStream.Close();
        }

        private static byte[] SerializeMessage(FileProperties file)
        {
            var binaryFormatter = new BinaryFormatter();
            using (var stream = new MemoryStream())
            {
                binaryFormatter.Serialize(stream, file);
                return stream.ToArray();
            }
        }

        private static bool IsFileLocked(FileInfo file)
        {
            FileStream stream = null;

            try
            {
                stream = file.Open(FileMode.Open, FileAccess.ReadWrite, FileShare.None);
            }
            catch (IOException)
            {
                return true;
            }
            finally
            {
                if (stream != null)
                    stream.Close();
            }

            return false;
        }
    }
}
