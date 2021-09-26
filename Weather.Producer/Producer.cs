using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Microsoft.Extensions.Logging;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Avro;
using Weather.DTO;
using System;

namespace Weather.Producer
{
    public interface IProducer<T>
    {
        void Send(string topic, T payload);
    }

    public class Producer<T> : IProducer<T> where T : class
    {
        private ProducerConfig _config;
        private ILogger<Producer<T>> _logger;

        public Producer(ProducerConfig config, ILogger<Producer<T>> logger)
        {
            _config = config;
            _logger = logger;
        }

        public void Send(string topic, T payload)
        {
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "localhost:8081"
            };

            var avroSerializerConfig = new AvroSerializerConfig
            {
                BufferBytes = 100
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<string, WeatherRecord>(_config)
                    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry, avroSerializerConfig))
                    .SetValueSerializer(new AvroSerializer<WeatherRecord>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                _logger.LogInformation($"{producer.Name} producing on {topic}.");

                int i = 0;
                string text;
                while ((text = Console.ReadLine()) != "q")
                {
                    //var WeatherRecord = new WeatherRecord { ID = i++, Firstname = text, Lastname = "Smithy", BirthDate = "02/12/45"};
                    WeatherRecord weatherRecord = new WeatherRecord { City = WeatherCities.Sydney, GmtOffset = 10, Type = WeatherTypes.Rainy, Temperature = 23, Humidity = 100, WindSpeed = 23 };
                    producer
                        .ProduceAsync(topic, new Message<string, WeatherRecord> { Key = text, Value = weatherRecord })
                        .ContinueWith(task =>
                        {
                            if (!task.IsFaulted)
                            {
                                Console.WriteLine($"produced to: {task.Result.TopicPartitionOffset}");
                            }

                            // Task.Exception is of type AggregateException. Use the InnerException property
                            // to get the underlying ProduceException. In some cases (notably Schema Registry
                            // connectivity issues), the InnerException of the ProduceException will contain
                            // additional information pertaining to the root cause of the problem. Note: this
                            // information is automatically included in the output of the ToString() method of
                            // the ProduceException which is called implicitly in the below.
                            Console.WriteLine($"error producing message: {task.Exception.InnerException}");
                        });

                }
            }
        }
    }
}
