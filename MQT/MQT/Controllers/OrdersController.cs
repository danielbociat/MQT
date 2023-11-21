using System.Diagnostics;
using Confluent.Kafka;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using MQT.Events;

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
        var orders = new List<Order>();

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
                
                var value = consumeResult.Message.Value;

                Console.WriteLine(value);

                try
                {
                    var v = JsonSerializer.Deserialize<Order>(value);
                    orders.Add(v);
                    Console.WriteLine("Success Deserialization!");
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                }

                items.Add(value);
            }
            watch.Stop();

            consumer.Close();
        }

        return Ok(orders);
    }
}