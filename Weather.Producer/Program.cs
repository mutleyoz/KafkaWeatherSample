﻿using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
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
                .AddSingleton( sp => new ProducerConfig { BootstrapServers = "localhost:29092", ClientId = Dns.GetHostName() })
                .AddLogging(config => config.AddConsole())
                .BuildServiceProvider();

            var producer = serviceProvider.GetService<IProducer<WeatherRecord>>();
            if(producer != null)
            {
                var weatherRecord = new WeatherRecord { City = CityName.Sydney, GmtOffset = 11, WeatherType = DTO.Weather.Fog, Humidity = 100, Temperature = 17, WindSpeed = 20 };

                producer.Send("weather", weatherRecord);
            }
        }
    }
}
