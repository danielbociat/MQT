using Confluent.Kafka;
using MQT.Events;
using System.Text.Json;

namespace MQT.BackgroundTask;
public class ConsumeKafka : BackgroundService
{
    private readonly ILogger<ConsumeKafka> _logger;
    private static readonly string _url = "localhost:9092";

    private readonly ConsumerConfig _consumerConfig = new()
    {
        BootstrapServers = _url,
        GroupId = "alwaysReadFullData1",
        AutoOffsetReset = AutoOffsetReset.Earliest,
    };

    public ConsumeKafka(ILogger<ConsumeKafka> logger)
    {
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("Timed Hosted Service running.");

        DoWork();

        using PeriodicTimer timer = new(TimeSpan.FromMilliseconds(1));

        try
        {
            while (await timer.WaitForNextTickAsync(stoppingToken))
            {
                DoWork();
            }
        }
        catch (OperationCanceledException)
        {
            _logger.LogInformation("Timed Hosted Service is stopping.");
        }
    }

    // Could also be a async method, that can be awaited in ExecuteAsync above
    private void DoWork()
    {
        var items = new List<string>();

        using var consumer = new ConsumerBuilder<Ignore, string>(_consumerConfig).Build();

        consumer.Subscribe("topicTest1");

        while (true)
        {
            var consumeResult = consumer.Consume();
            var value = consumeResult.Message.Value;

            //Console.WriteLine(value);

            try
            {
                var order = JsonSerializer.Deserialize<Order>(value);

                if(order is not null)
                {
                    HandleLengthOrder(order, value);

                    ProduceMessage($"clientTopics-{order.Client.Id}", value);
                    ProduceMessage("productsTopics", value);
                    ProduceMessage("lengthTopics", value);
                }
               
                //Console.WriteLine("Success Deserialization!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            items.Add(value);
        }
    }

    private static void ProduceMessage(string topic, string message)
    {
        var config = new ProducerConfig
        {
            BootstrapServers = _url,
        };

        using var producer = new ProducerBuilder<Null, string>(config).Build();

        var produceResult = producer.ProduceAsync(topic, new Message<Null, string> { Value = message }).Result;
        Console.WriteLine($"Produced message to topic '{produceResult.Topic}', partition {produceResult.Partition}, offset {produceResult.Offset}");
    }


    private void HandleLengthOrder(Order order, string value)
    {
        if(order.DeliveryTime is null)
        {
            return;
        }

        var exists = TryGetLast("shortest", out var shortest);
        if ((exists && GetOrderTime(shortest) > GetOrderTime(order)) || !exists)
        {
            ProduceMessage("shortest", value);
        }

        exists = TryGetLast("longest", out var longest);
        if((exists && GetOrderTime(longest) < GetOrderTime(order)) || !exists)
        {
            ProduceMessage("longest", value);
        }
    }

    private bool TryGetLast(string topic, out Order? order)
    {
        var config = new ConsumerConfig
        {
            GroupId = "getLast3",
            BootstrapServers = _url,
            AutoOffsetReset = AutoOffsetReset.Latest,
            EnableAutoCommit = false
        };

        using var consumer = new ConsumerBuilder<Ignore, string>(config).Build();

        try
        {
            consumer.Subscribe(topic);
            //consumer.Assign(new TopicPartitionOffset(topic, new Partition(0), Offset.Beginning));

            var consumeResult = consumer.Consume();
            var value = consumeResult.Message.Value;

            Console.WriteLine(value);

            try
            {
                order = JsonSerializer.Deserialize<Order>(value);

                if (order is not null)
                {
                    Console.WriteLine("Success Deserialization!");
                    return true;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            
           
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);

        }
        finally
        {
            consumer.Close();
        }

        order = null;
        return false;
    } 

    private static long GetOrderTime(Order order) => order.DeliveryTime!.Value.Ticks - order.CreatedTime.Ticks;

}