using MQT.Events;

namespace MQT.Services;

public interface IKafkaOrderConsumerService
{
    bool TryGetLastShortestOrder(out Order? order);

    bool TryGetLastLongestOrder(out Order? order);
    bool TryGetLastProductsDictionary(out Dictionary<string, int>? productsDictionary);
}