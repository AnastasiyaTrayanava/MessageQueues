using RabbitMQ.Client;
using System;
using System.IO;
using System.Text;
using RabbitMQ.Client.Events;

namespace MessageQueues.Processing
{
    internal class Program
    {
        private const string _writingFolder = @"C:\Outbox";
        private const string _fileFormat = ".txt";

        static void Main(string[] args)
        {
            var factory = new ConnectionFactory { HostName = "localhost" };
            using var connection = factory.CreateConnection();
            using var channel = connection.CreateModel();

            channel.QueueDeclare(queue: "hello",
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            Console.WriteLine("Waiting for messages.");

            var consumer = new EventingBasicConsumer(channel);
            consumer.Received += (model, e) =>
            {
                var body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                Console.WriteLine("Received file");

                var fileCount = Directory.GetFiles(_writingFolder).Length;
                var newFilePath = @$"{_writingFolder}\received_{fileCount}{_fileFormat}";
                File.WriteAllText(newFilePath, message);

                Console.WriteLine($"File written: {newFilePath}");
            };
            channel.BasicConsume(queue: "hello",
                autoAck: true,
                consumer: consumer);

            Console.ReadLine();
        }
    }
}
