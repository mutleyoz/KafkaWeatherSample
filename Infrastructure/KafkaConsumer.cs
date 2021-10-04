using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using System;
using System.Threading;

namespace Infrastructure
{
    public class KafkaConsumer<T> : IDisposable where T : class
    {
        private SchemaRegistryConfig _schemaRegistryConfig;
        private ISchemaRegistryClient _schemaRegistryClient;
        private ConsumerConfig _consumerConfig;
        private IConsumer<string, T> _consumer;

        public KafkaConsumer(SchemaRegistryConfig schemaRegistryConfig, ConsumerConfig consumerConfig)
        {
            _schemaRegistryConfig = schemaRegistryConfig;
            _consumerConfig = consumerConfig;
            _schemaRegistryClient = new CachedSchemaRegistryClient(_schemaRegistryConfig);

            _consumer =
                new ConsumerBuilder<string, T>(_consumerConfig)
                    .SetKeyDeserializer(new AvroDeserializer<string>(_schemaRegistryClient).AsSyncOverAsync())
                    .SetValueDeserializer(new AvroDeserializer<T>(_schemaRegistryClient).AsSyncOverAsync())
                    .SetErrorHandler((_, e) => Console.WriteLine($"Consumer Error: {e.Reason}"))
                    .Build();
        }

        public KafkaConsumer<T> Subscribe(string topic)
        {
            _consumer.Subscribe(topic);
            return (this);
        }


        public void Listen(CancellationTokenSource cts, Action<T> consumeEvent)
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
