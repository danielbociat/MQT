using System;
using System.Collections.Generic;
using System.Diagnostics;
using Confluent.Kafka;
using Microsoft.AspNetCore.Mvc;

namespace MQT.Controllers;

[ApiController]
[Route("/api/orders")]
public class OrdersController: ControllerBase
{
    private readonly ConsumerConfig _consumerConfig;
    
    public OrdersController()
    {
        _consumerConfig = new ConsumerConfig
        {
            BootstrapServers = "localhost:9092",
            GroupId = "foo",
            AutoOffsetReset = AutoOffsetReset.Earliest
        };
    }

    [HttpGet]
    public IActionResult ConsumeItemsForTime(int seconds)
    {
        var items = new List<string>();
        
        using (var consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build())
        {
            consumer.Subscribe("topicTest1");

            Stopwatch watch = new Stopwatch();
            watch.Start();

            var cancelled = false;
            
            while (!cancelled)
            {
                var consumeResult = consumer.Consume();

                if (seconds > 0 && watch.ElapsedMilliseconds / 1000 >= seconds)
                {
                    cancelled = true;
                }
                
                Console.WriteLine(consumeResult.Message.Value);
                
                items.Add(consumeResult.Message.Value);
            }
            watch.Stop();

            consumer.Close();
        }

        return Ok(items);
    }
}