using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Infrastructure;
using Serilog;
using System;
using System.Threading;
using Weather.DTO;


namespace Weather.Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            Log.Logger = new LoggerConfiguration()
                .WriteTo.Console()
                .CreateLogger();
            
            var producerConfig = new ProducerConfig { BootstrapServers = "localhost:9092" };
            var schemaRegistryConfig = new SchemaRegistryConfig { Url = "localhost:8081" };


            Log.Information("--Starting up producer--");
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
                    Thread.Sleep(10);
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
