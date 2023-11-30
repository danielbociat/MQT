using System.Text.Json;
using Confluent.Kafka;
using MQT.Events;

namespace MQT.Services;

public class KafkaOrderConsumerService : IKafkaOrderConsumerService
{
    private static readonly string _url = "localhost:9092";

    public bool TryGetLastShortestOrder(out Order? order)
        => TryGetLast("shortest", out order);
    
    public bool TryGetLastLongestOrder(out Order? order)
        => TryGetLast("longest", out order);
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
            var partition = new TopicPartition(topic, new Partition(0));
            var offsets = consumer.QueryWatermarkOffsets(partition, TimeSpan.FromMinutes(1));

            var desiredOffset = new Offset(0);

            if (offsets.High > 0)
            {
                desiredOffset = new Offset(offsets.High.Value - 1);
            }
            
            consumer.Assign(partition);
            consumer.Seek(new TopicPartitionOffset(partition, desiredOffset));

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
}