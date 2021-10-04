using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Net;
using System.Threading;
using Weather.DTO;


namespace Weather.Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = "localhost:8081" };

            using (var client = new KafkaClient<WeatherRecord>(schemaRegistryConfig, producerConfig))
            {
                while (true)
                {
                    var weather = new WeatherRecord
                    {
                        DateTime = DateTime.Now.ToLongTimeString(),
                        City = (WeatherCities)GetRandom(0, 4),
                        GmtOffset = 10,
                        Temperature = GetRandom(5, 40),
                        Type = (WeatherTypes)GetRandom(0, 4),
                        WindSpeed = GetRandom(0, 40)
                    };

                    client.Producer.Send("weather", "weatherrecord", weather);
                    Thread.Sleep(100);
                }
            }
        }

        private static int GetRandom(int lower, int higher)
        {
            var randomiser = new Random(DateTime.Now.Millisecond);
            return randomiser.Next(lower, higher);
        }
    }
}
