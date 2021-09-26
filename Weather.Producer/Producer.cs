using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Microsoft.Extensions.Logging;
using System;
using Weather.DTO;

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

                WeatherRecord weatherRecord = new WeatherRecord { City = WeatherCities.Sydney, GmtOffset = 10, Type = WeatherTypes.Rainy, Temperature = 23, Humidity = 100, WindSpeed = 23 };
                producer
                    .ProduceAsync(topic, new Message<string, WeatherRecord> { Key = "weatherrecord", Value = weatherRecord })
                    .ContinueWith(task =>
                    {
                        if (!task.IsFaulted)
                        {
                            _logger.LogInformation($"produced to: {task.Result.TopicPartitionOffset}");
                        }
                        else
                        {
                            _logger.LogError($"error producing message: {task.Exception.InnerException}");
                        }
                    });
            }
        }
    }
}
