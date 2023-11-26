using Confluent.Kafka;
using MQT.Events;
using System.Text.Json;

namespace MQT.BackgroundTask;
public class ConsumeKafka : BackgroundService
{
    private readonly ILogger<ConsumeKafka> _logger;

    private readonly ConsumerConfig _consumerConfig = new()
    {
        BootstrapServers = "localhost:9092",
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

        using PeriodicTimer timer = new(TimeSpan.FromSeconds(1));

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

            Console.WriteLine(value);

            try
            {
                var order = JsonSerializer.Deserialize<Order>(value);

                Console.WriteLine("Success Deserialization!");
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }

            items.Add(value);
        }
    }
}