using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;

namespace Infrastructure
{

    public class KafkaClient: IDisposable
    {
        private readonly SchemaRegistryConfig _schemaRegistryConfig;
        private readonly ProducerConfig _producerConfig;
        private readonly ConsumerConfig _consumerConfig;

        public KafkaConsumer Consumer { get; set; }
        public KafkaProducer Producer { get; init; }

        public KafkaClient(SchemaRegistryConfig schemaRegistryConfig, ProducerConfig producerConfig = null, ConsumerConfig consumerConfig = null)
        {
            _schemaRegistryConfig = schemaRegistryConfig ?? new SchemaRegistryConfig { Url = "localhost:8081" };
            _producerConfig = producerConfig ?? new ProducerConfig { BootstrapServers = "localhost:9092" };
            _consumerConfig = consumerConfig ?? new ConsumerConfig { BootstrapServers = "localhost:9092", GroupId = "sc-consumer-grp" };

            Producer = new KafkaProducer(_schemaRegistryConfig, _producerConfig);
            Consumer = new KafkaConsumer(_schemaRegistryConfig, _consumerConfig);
        }

        public void Dispose()
        {
            Consumer.Dispose();
            Producer.Dispose();
        }
    }
}
