using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using System;
using System.Net;
using Weather.DTO;

namespace Weather.Producer
{
    class Program
    {
        static void Main(string[] args)
        {
            var serviceProvider = new ServiceCollection()
                .AddScoped(typeof(IProducer<>), typeof(Producer<>))
                .AddSingleton( sp => new ProducerConfig { BootstrapServers = "localhost:9092", ClientId = Dns.GetHostName() })
                .AddLogging(config => config.AddConsole())
                .BuildServiceProvider();

            var producer = serviceProvider.GetService<IProducer<WeatherRecord>>();
            if(producer != null)
            {
                var weatherRecord = new WeatherRecord { City = WeatherCities.Sydney, DateTime = DateTime.Now.ToLongTimeString(), GmtOffset = 11, Type = WeatherTypes.Rainy, Humidity = 100, Temperature = 17, WindSpeed = 20 };

                producer.Send("weatherrecord.V2", weatherRecord);
            }
        }
    }
}
