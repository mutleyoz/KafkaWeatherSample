using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Threading;
using Weather.DTO;

namespace Infrastructure
{
    public class KafkaConsumer : IDisposable
    {
        private SchemaRegistryConfig _schemaRegistryConfig;
        private ISchemaRegistryClient _schemaRegistryClient;
        private ConsumerConfig _consumerConfig;
        private AvroSerializerConfig _avroSerializerConfig;
        private IConsumer<string, WeatherRecord> _consumer;

        public KafkaConsumer(SchemaRegistryConfig schemaRegistryConfig, ConsumerConfig consumerConfig)
        {
            _schemaRegistryConfig = schemaRegistryConfig;
            _consumerConfig = consumerConfig;
            _avroSerializerConfig = new AvroSerializerConfig { BufferBytes = 100 };
            _schemaRegistryClient = new CachedSchemaRegistryClient(_schemaRegistryConfig);

            _consumer =
                new ConsumerBuilder<string, WeatherRecord>(_consumerConfig)
                    .SetKeyDeserializer(new AvroDeserializer<string>(_schemaRegistryClient, _avroSerializerConfig).AsSyncOverAsync())
                    .SetValueDeserializer(new AvroDeserializer<WeatherRecord>(_schemaRegistryClient, _avroSerializerConfig).AsSyncOverAsync())
                    .SetErrorHandler((_, e) => Console.WriteLine($"Consumer Error: {e.Reason}"))
                    .Build();
        }

        public KafkaConsumer Subscribe(string topic)
        {
            _consumer.Subscribe(topic);
            return (this);
        }


        public void Listen(CancellationTokenSource cts, Action<WeatherRecord> consumeEvent)
        {
            while (true)
            {
                var message = _consumer.Consume(cts.Token);
                if(consumeEvent != null)
                {
                    consumeEvent(message.Message.Value);
                }
            }
        }
        public void Dispose()
        {
            _schemaRegistryClient.Dispose();
            _consumer.Dispose();
        }

    }
}
