using RabbitMQ.Client;
using System;
using System.IO;
using System.Text;
using RabbitMQ.Client.Events;
using System.Runtime.Serialization.Formatters.Binary;
using MessageQueues.Common.Models;

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

        // mark chunks and combine them into one big file
        private static void CreateChunkedMessagesReceiver()
        {
            var consumer = new EventingBasicConsumer(_channel);
            consumer.Received += (model, e) =>
            {
                var body = e.Body.ToArray();
                var deserializedMessage = DeserializeMessage(body);
                var fileChunkBody = Encoding.UTF8.GetString(deserializedMessage.Body);

                Console.WriteLine("Received a chunk");

                var newFilePath = CreateFileName(deserializedMessage.ChunkNumber, deserializedMessage.Name);
                File.WriteAllText(newFilePath, fileChunkBody);

                Console.WriteLine($"Chunk written: {newFilePath}");

                // or amount of chunks is equal to totalchunks
                if (deserializedMessage.ChunkNumber == deserializedMessage.TotalChunks)
                {
                    CombineChunks(deserializedMessage.Name, deserializedMessage.TotalChunks);
                }
            };
            _channel.BasicConsume(queue: "largequeue",
                autoAck: true,
                consumer: consumer);
        }

        private static string CreateFileName(int chunkNumber, string name)
        {
            var newFilePath = @$"{_writingFolder}\{chunkNumber}_{name}";
            return newFilePath;
        }

        private static void CombineChunks(string fileName, int totalChunks)
        {
            var endFile = $@"{_writingFolder}\{fileName}";
            var files = Directory.GetFiles(_writingFolder, $"*{fileName}");

            var stream = new FileStream(endFile, FileMode.Append);
            for (var i = 1; i <= totalChunks; i++)
            {
                Console.WriteLine($"Writing chunk #{i}");
                var fileChunk = CreateFileName(i, fileName);
                var array = File.ReadAllBytes(fileChunk);
                stream.Write(array, 0, array.Length);
            }
            stream.Close();
            Console.WriteLine($"File writing complete. Path: {endFile}");
            foreach (var file in files)
            {
                File.Delete(file);
            }
        }

        private static FileProperties DeserializeMessage(byte[] byteMessage)
        {
            using (var stream = new MemoryStream(byteMessage))
            {
                var binaryFormatter = new BinaryFormatter();
                return (FileProperties)binaryFormatter.Deserialize(stream);
            }
        }
    }
}
