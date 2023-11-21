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
            GroupId = "alwaysReadFullData",
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };
    }

    [HttpGet]
    public IActionResult GetOrdersForCustomer(string clientId)
    {
        var list = new List<Order>();
        GetOrders(o => { AddOrderToList(o, list, clientId); });
        return Ok(list);
    }

    private static void AddOrderToList(Order order, IList<Order> orders, string clientId)
    {
        if (order.Client.Id.Equals(clientId))
        {
            orders.Add(order);
        }
    }

    private void GetOrders(Action<Order> func)
    {
        var items = new List<string>();

        using var consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build();

        consumer.Subscribe("topicTest1");

        var time = TimeSpan.FromMilliseconds(10000);
        while (true)
        {
            var consumeResult = consumer.Consume(time);
            if (consumeResult is null)
            {
                break;
            }

            var value = consumeResult.Message.Value;

            Console.WriteLine(value);

            try
            {
                var order = JsonSerializer.Deserialize<Order>(value);

                if(order is not null)
                {
                    func(order);
                }

                Console.WriteLine("Success Deserialization!");
            }
            catch (Exception)
            { }

            items.Add(value);

            time = TimeSpan.FromMilliseconds(100);
        }

        consumer.Close();
    }
}