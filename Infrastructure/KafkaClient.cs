using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;

namespace Infrastructure
{
    public class KafkaClient<T> : IDisposable where T : class
    {
        private readonly SchemaRegistryConfig _schemaRegistryConfig;
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;

        public KafkaConsumer<T> Consumer { get; set; }
        public KafkaProducer<T> Producer { get; init; }

        public KafkaClient(SchemaRegistryConfig schemaRegistryConfig, ProducerConfig producerConfig = null, ConsumerConfig consumerConfig = null)
        {
            _schemaRegistryConfig = schemaRegistryConfig ?? new SchemaRegistryConfig { Url = "localhost:8081" };
            _producerConfig = producerConfig;
            _consumerConfig = consumerConfig;

            Producer = producerConfig != null ? new KafkaProducer<T>(_schemaRegistryConfig, _producerConfig) : null;
            Consumer = consumerConfig != null ? new KafkaConsumer<T>(_schemaRegistryConfig, _consumerConfig) : null;
        }

        public void Dispose()
        {
            Consumer.Dispose();
            Producer.Dispose();
        }
    }
}
