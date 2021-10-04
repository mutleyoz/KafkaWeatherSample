using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Threading;
using Weather.DTO;
using System.Text.Json;
using Infrastructure;

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

            var kafkaClient = new KafkaClient(schemaRegistryConfig, null, consumerConfig);
            kafkaClient.Consumer.Subscribe("weather").Listen(cts, msg =>
            {
                var serializedResult = JsonSerializer.Serialize(msg);
                Console.WriteLine(serializedResult);
            });
        }
    }
}
