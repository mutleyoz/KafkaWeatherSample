using Confluent.Kafka;
using Confluent.Kafka.SyncOverAsync;
using Microsoft.Extensions.Logging;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Avro;

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

        public async void Send(string topic, T payload)
        {
            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                Url = "localhost:8090"
            };

            var avroSerializerConfig = new AvroSerializerConfig
            {
                BufferBytes = 100
            };

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            {
                using (var producer = new ProducerBuilder<string, T>(_config)
                    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry, avroSerializerConfig))
                    .SetValueSerializer(new AvroSerializer<T>(schemaRegistry, avroSerializerConfig))
                    .Build())
                {
                    _logger.LogInformation($"Sending message to topic");

                    await producer.ProduceAsync("weather", new Message<string, T> { Key = "weather_message", Value = payload })
                         .ContinueWith(task =>
                         {
                             if (task.IsFaulted)
                             {
                                 _logger.LogError($"Broker faulted : {task.Exception.Message}");
                             }
                             else
                             {
                                 _logger.LogInformation($"Wrote to offset: {task.Result.Offset}.");
                             }
                         });
  
                }
            }
        }
    }
}
