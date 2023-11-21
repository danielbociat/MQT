using System;
using System.Collections.Generic;

namespace MQT.Events;

public class Order
{
    public DateTime CreatedTime { get; set; }
    public DateTime? DeliveryTime { get; set; }
    public string Id { get; set; }
    public Client Client { get; set; }
    public string Address { get; set; }
    public IEnumerable<Tuple<Product, int>> ProductQuantities { get; set; }
}
