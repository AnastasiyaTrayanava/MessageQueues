using System;
using System.IO;
using System.Threading.Tasks;
using RabbitMQ.Client;

namespace MessageQueues.DataCapture
{
    internal class Program
    {
        private const string _listeningFolder = @"C:\Inbox";
        private const string _fileFormats = "*.txt";
        private static IModel _channel;
        private static FileSystemWatcher _watcher;

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory { HostName = "localhost" };
            using var connection = factory.CreateConnection();
            _channel = connection.CreateModel();

            _channel.QueueDeclare(queue: "hello",
                durable: false,
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
            var file = await File.ReadAllBytesAsync(filePath);

            _channel.BasicPublish(exchange: string.Empty,
                routingKey: "hello",
                basicProperties: null,
                body: file);
            Console.WriteLine($"File sent: {filePath}");
        }
    }
}
