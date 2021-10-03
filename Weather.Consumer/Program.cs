using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Threading;
using Weather.DTO;

namespace Weather.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:9092",
                GroupId = "sc-consumer-grp",
                AllowAutoCreateTopics = true
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "localhost:8081"
            };

            var avroSerializerConfig = new AvroSerializerConfig
            {
                // optional Avro serializer properties:
                BufferBytes = 100
            };


            CancellationTokenSource cts = new CancellationTokenSource();

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer =
                new ConsumerBuilder<string, User.DTO.User>(consumerConfig)
                    .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                    .SetValueDeserializer(new AvroDeserializer<User.DTO.User>(schemaRegistry).AsSyncOverAsync())
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build())
            {
                consumer.Subscribe("test");

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cts.Token);
                            Console.WriteLine($"weather key: {consumeResult.Message.Key}, DateTime: {consumeResult.Message.Value.name}, City: {consumeResult.Message.Value.favorite_number}, Type: {consumeResult.Message.Value.favorite_color}");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    Console.WriteLine("Operation cancelled.");
                }
                finally
                {
                    cts.Cancel();
                    consumer.Close();
                }
            }
        }
    }
}
