using RabbitMQ.Client;
using System;
using System.Collections.Generic;
using System.IO;

namespace MessageQueues.LargeDataCapture
{
    internal class Program
    {
        private const string _listeningFolder = @"C:\Inbox";
        private const string _fileFormats = "*.txt";
        private static IModel _channel;
        private static FileSystemWatcher _watcher;
        private const string _queueName = "largequeue";

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
            SendMessage(e.FullPath);
        }

        private static void OnCreated(object sender, FileSystemEventArgs e)
        {
            Console.WriteLine($"Created: {e.FullPath}");
            SendMessage(e.FullPath);
        }

        private static void SendMessage(string filePath)
        {
            const int chunkSize = 4096;
            var count = 0;

            var fileStream = File.OpenRead(filePath);
            var streamReader = new StreamReader(fileStream);

            var fileSize = fileStream.Length;
            var remainingFileSize = Convert.ToInt32(fileStream.Length);
            byte[] buffer;

            while (remainingFileSize > 0)
            {
                int read;

                if (remainingFileSize > chunkSize)
                {
                    buffer = new byte[chunkSize];
                    read = fileStream.Read(buffer, 0, chunkSize);
                }
                else
                {
                    buffer = new byte[remainingFileSize];
                    read = fileStream.Read(buffer, 0, remainingFileSize);
                }

                count++;
                var fileName = $"largeFile_chunk{count}.txt";

                var basicProperties = _channel.CreateBasicProperties();
                basicProperties.Persistent = true;
                basicProperties.Headers = new Dictionary<string, object>();
                basicProperties.Headers.Add("output-file", fileName);

                _channel.BasicPublish(
                    string.Empty, 
                    _queueName, 
                    basicProperties, 
                    buffer);

                Console.WriteLine($"File sent: {filePath}");
                remainingFileSize -= read;
            }

            streamReader.Close();
            fileStream.Close();
        }
    }
}
