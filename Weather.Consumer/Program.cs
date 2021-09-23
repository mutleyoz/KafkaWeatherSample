using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Weather.Consumer
{
    class Program
    {
        static void Main(string[] args)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = "localhost:29092",
                GroupId = "weather_group",
                AllowAutoCreateTopics = true
            };

            var avroSerializerConfig = new AvroSerializerConfig
            {
                // optional Avro serializer properties:
                BufferBytes = 100
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "localhost:8090"

            };

            CancellationTokenSource cts = new CancellationTokenSource();

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var consumer =
                new ConsumerBuilder<string, DTO.Weather>(consumerConfig)
                    .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                    .SetValueDeserializer(new AvroDeserializer<DTO.Weather>(schemaRegistry).AsSyncOverAsync())
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build())
            {
                consumer.Subscribe("weather");

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cts.Token);
                            Console.WriteLine($"weather key: {consumeResult.Message.Key}, favorite color: {consumeResult.Message.Value}");
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
                    consumer.Close();
                }
            }
        }
    }
}
