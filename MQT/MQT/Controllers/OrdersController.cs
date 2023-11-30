using System.Diagnostics;
using Confluent.Kafka;
using System.Text.Json;
using Microsoft.AspNetCore.Mvc;
using MQT.Events;
using MQT.Services;

namespace MQT.Controllers;

[ApiController]
[Route("/api/orders")]
public class OrdersController: ControllerBase
{
    private readonly IKafkaOrderConsumerService _kafkaOrderConsumerService;

    private readonly ConsumerConfig _consumerConfig = new()
    {
        BootstrapServers = "localhost:9092",
        GroupId = "alwaysReadFullData",
        AutoOffsetReset = AutoOffsetReset.Earliest,
        EnableAutoCommit = false
    };

    public OrdersController(IKafkaOrderConsumerService kafkaOrderConsumerService)
    {
        _kafkaOrderConsumerService = kafkaOrderConsumerService;
    }

    [HttpGet("shortandlong")]
    public IActionResult GetShortestAndLongestOrder()
    {
        Order? shortestOrder = null;
        Order? longestOrder = null;
        _kafkaOrderConsumerService.TryGetLastShortestOrder(out shortestOrder);
        _kafkaOrderConsumerService.TryGetLastLongestOrder(out longestOrder);
        
        return Ok(new { shortestOrder, longestOrder });
    }
    
    [HttpGet("{clientId}")]
    public IActionResult GetOrdersForCustomer([FromRoute] string clientId)
    {
        return Ok(GetOrders(clientId));
    }

    [HttpGet("topProducts/{productsNumber}")]
    public IActionResult GetTopProducts([FromRoute] int productsNumber)
    {
        var dict = new Dictionary<Product, int>();
        // GetOrders(o => { GetProducts(o, dict); });
        // var topProducts = dict.OrderByDescending(kv => kv.Value).ToList().Take(productsNumber);
        return Ok(dict);
    }

    private static void GetProducts(Order order, Dictionary<Product, int> dict)
    {
        foreach(var (k,v) in order.ProductQuantities)
        {
            if(dict.TryGetValue(k, out var _))
            {
                dict[k] += v;
            }
            else
            {
                dict.Add(k,v);
            }
        }
    }

    private List<Order> GetOrders(string clientId)
    {
        using var consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build();

        consumer.Subscribe($"clientTopics-{clientId}");

        var time = TimeSpan.FromMilliseconds(1000);
        var clientOrders = new List<Order>();
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

                if (order is not null)
                {
                    clientOrders.Add(order);
                }

                Console.WriteLine("Success Deserialization!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            time = TimeSpan.FromMilliseconds(100);
        }

        consumer.Close();
        return clientOrders;
    }
}