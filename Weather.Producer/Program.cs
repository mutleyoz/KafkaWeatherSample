using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Net;
using Weather.DTO;


namespace Weather.Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var producerConfig = new ProducerConfig
            {
                BootstrapServers = "localhost:9092"
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

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<string, User.DTO.User>(producerConfig)
                    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry, avroSerializerConfig))
                    .SetValueSerializer(new AvroSerializer<User.DTO.User>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                Console.WriteLine($"{producer.Name} producing on 'test'. Enter user names, q to exit.");

                int i = 0;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    User.DTO.User user = new User.DTO.User { name = text, favorite_color = "green", favorite_number = i++, hourly_rate = new Avro.AvroDecimal(67.99) };
                    producer
                        .ProduceAsync("test", new Message<string, User.DTO.User> { Key = text, Value = user })
                        .ContinueWith(task =>
                        {
                            if (!task.IsFaulted)
                            {
                                Console.WriteLine($"produced to: {task.Result.TopicPartitionOffset}");
                            }
                            else
                            {
                                Console.WriteLine($"error producing message: {task.Exception.InnerException}");
                            }

                            // Task.Exception is of type AggregateException. Use the InnerException property
                            // to get the underlying ProduceException. In some cases (notably Schema Registry
                            // connectivity issues), the InnerException of the ProduceException will contain
                            // additional information pertaining to the root cause of the problem. Note: this
                            // information is automatically included in the output of the ToString() method of
                            // the ProduceException which is called implicitly in the below.
                           
                        });
                }
            }
        }
    }
}
