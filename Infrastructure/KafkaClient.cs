using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;

namespace Infrastructure
{
    public class KafkaClient<T> : IDisposable where T : class
    {
        private readonly SchemaRegistryConfig _schemaRegistryConfig;

        public KafkaConsumer<T> Consumer { get; set; }
        public KafkaProducer<T> Producer { get; init; }

        public KafkaClient(SchemaRegistryConfig schemaRegistryConfig, ProducerConfig producerConfig = null )
        {
            _schemaRegistryConfig = schemaRegistryConfig ?? new SchemaRegistryConfig { Url = "localhost:8081" };

            Producer = producerConfig != null ? new KafkaProducer<T>(_schemaRegistryConfig, producerConfig) : null;
        }

        public KafkaClient(SchemaRegistryConfig schemaRegistryConfig, ConsumerConfig consumerConfig = null)
        {
            _schemaRegistryConfig = schemaRegistryConfig ?? new SchemaRegistryConfig { Url = "localhost:8081" };

            Consumer = consumerConfig != null ? new KafkaConsumer<T>(_schemaRegistryConfig, consumerConfig) : null;
        }

        public void Dispose()
        {
            Consumer.Dispose();
            Producer.Dispose();
        }
    }
}
