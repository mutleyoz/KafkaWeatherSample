using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using Serilog;

namespace Infrastructure
{
    public class KafkaProducer<T> : KafkaBase<T>, IDisposable where T : class
    {
        private ISchemaRegistryClient _schemaRegistryClient;
        private ProducerConfig _producerConfig;
        private IProducer<string, T> _producer;

        public KafkaProducer(SchemaRegistryConfig schemaRegistryConfig, ProducerConfig producerConfig)
        {
            _producerConfig = producerConfig;

            var avroSerializerConfig = new AvroSerializerConfig {BufferBytes = 100 };

            _schemaRegistryClient = new CachedSchemaRegistryClient(schemaRegistryConfig);

            _producer =
                new ProducerBuilder<string, T>(_producerConfig)
                    .SetKeySerializer(new AvroSerializer<string>(_schemaRegistryClient, avroSerializerConfig))
                    .SetValueSerializer(new AvroSerializer<T>(_schemaRegistryClient, avroSerializerConfig))
                    .Build();

            Log.Information($"{nameof(KafkaProducer<T>)} initialised");
        }

        public override void Send(string topic, string key, T message)
        {

            Log.Information($"{nameof(KafkaProducer<T>)} - Sending {message}");

            _producer
                    .ProduceAsync(topic, new Message<string, T> { Key = key, Value = message })
                    .ContinueWith(task =>
                    {
                        if (task.IsFaulted)
                        { 
                            Log.Error($"{nameof(KafkaProducer<T>)} error : {task.Exception.InnerException}");
                        }
                    });
        }

        public void Dispose()
        {
            _schemaRegistryClient.Dispose();
            _producer.Dispose();
        }
    }
}
