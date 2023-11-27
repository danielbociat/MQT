using MQT.Events;
using System.Collections.Concurrent;

namespace MQT.Statistics;

public class Stats
{
    public ConcurrentDictionary<Product, int> Products { get; } = new ConcurrentDictionary<Product, int>();
    public Order ShortestOrder { get; set; } = default!;
    public Order LongestOrder { get; set; } = default!;
}
