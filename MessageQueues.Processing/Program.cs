using RabbitMQ.Client;
using System;
using System.IO;
using System.Text;
using RabbitMQ.Client.Events;
using System.Collections.Generic;
using System.Threading.Channels;

namespace MessageQueues.Processing
{
    internal class Program
    {
        private const string _writingFolder = @"C:\Outbox";
        private const string _fileFormat = ".txt";
        private static IModel _channel;

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

            Console.WriteLine("Waiting for messages.");

            CreateQueueReceiver();
            CreateChunkedMessagesReceiver();

            Console.ReadLine();
        }

        private static void CreateQueueReceiver()
        {
            var consumer = new EventingBasicConsumer(_channel);
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
            _channel.BasicConsume(queue: "hello",
                autoAck: true,
                consumer: consumer);
        }

        private static void CreateChunkedMessagesReceiver()
        {
            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += (model, e) =>
            {
                var body = e.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                Console.WriteLine($"Received a chunk");

                var fileCount = Directory.GetFiles(_writingFolder).Length;
                var newFilePath = @$"{_writingFolder}\largefile_received_{fileCount}{_fileFormat}";
                File.WriteAllTextAsync(newFilePath, message);

                Console.WriteLine($"File written: {newFilePath}");
            };
            _channel.BasicConsume(queue: "largequeue",
                autoAck: true,
                consumer: consumer);

           /* model.BasicQos(0, 1, false);
            QueueingBasicConsumer consumer = new QueueingBasicConsumer(model);
            model.BasicConsume(RabbitMqService.ChunkedMessageBufferedQueue, false, consumer);
            while (true)
            {
                BasicDeliverEventArgs deliveryArguments = consumer.Queue.Dequeue() as BasicDeliverEventArgs;
                Console.WriteLine("Received a chunk!");
                IDictionary<string, object> headers = deliveryArguments.BasicProperties.Headers;
                string randomFileName = Encoding.UTF8.GetString((headers["output-file"] as byte[]));
                bool isLastChunk = Convert.ToBoolean(headers["finished"]);
                string localFileName = string.Concat(@"c:\", randomFileName);
                using (FileStream fileStream = new FileStream(localFileName, FileMode.Append, FileAccess.Write))
                {
                    fileStream.Write(deliveryArguments.Body, 0, deliveryArguments.Body.Length);
                    fileStream.Flush();
                }
                Console.WriteLine("Chunk saved. Finished? {0}", isLastChunk);
                model.BasicAck(deliveryArguments.DeliveryTag, false);
            }*/
        }
    }
}
