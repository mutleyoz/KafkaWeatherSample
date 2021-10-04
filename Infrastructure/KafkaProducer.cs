using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using Weather.DTO;

namespace Infrastructure
{
    public class KafkaProducer : IDisposable
    {
        private SchemaRegistryConfig _schemaRegistryConfig;
        private ISchemaRegistryClient _schemaRegistryClient;

        private ProducerConfig _producerConfig;
        private AvroSerializerConfig _avroSerializerConfig;
        private IProducer<string, WeatherRecord> _producer;


        public KafkaProducer(SchemaRegistryConfig schemaRegistryConfig, ProducerConfig producerConfig )
        {
            _schemaRegistryConfig = schemaRegistryConfig;
            _producerConfig = producerConfig;

            _avroSerializerConfig = new AvroSerializerConfig
            {
                // optional Avro serializer properties:
                BufferBytes = 100
            };

            _schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);

            _producer =
                new ProducerBuilder<string, WeatherRecord>(_producerConfig)
                    .SetKeySerializer(new AvroSerializer<string>(_schemaRegistryClient, _avroSerializerConfig))
                    .SetValueSerializer(new AvroSerializer<WeatherRecord>(_schemaRegistryClient, _avroSerializerConfig))
                    .Build();
        }

        public void Dispose()
        {
            _schemaRegistryClient.Dispose();
            _producer.Dispose();
        }

        public void Send(string topic, string key, WeatherRecord weather)
        {
            _producer
                    .ProduceAsync(topic, new Message<string, WeatherRecord> { Key = key, Value = weather })
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
                    });
        }

    }
}
