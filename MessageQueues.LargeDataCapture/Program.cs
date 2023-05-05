using RabbitMQ.Client;
using System;
using System.IO;
using System.Reflection;
using System.Threading.Tasks;

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

        private static async void OnChanged(object sender, FileSystemEventArgs e)
        {
            if (e.ChangeType != WatcherChangeTypes.Changed)
            {
                return;
            }
            Console.WriteLine($"Changed: {e.FullPath}");
            await SendMessage(e.FullPath);
        }

        private static async void OnCreated(object sender, FileSystemEventArgs e)
        {
            Console.WriteLine($"Created: {e.FullPath}");
            await SendMessage(e.FullPath);
        }

        private static async Task SendMessage(string filePath)
        {
            const int chunkSize = 4096;
            var file = await File.ReadAllBytesAsync(filePath);

            IBasicProperties basicProperties = _channel.CreateBasicProperties();
            basicProperties.Persistent = true;

            _channel.BasicPublish(exchange: string.Empty,
                routingKey: _queueName,
                basicProperties: basicProperties,
                body: file);
            Console.WriteLine($"File sent: {filePath}");
        }
    }
}
