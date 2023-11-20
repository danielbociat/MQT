using System;
using System.Collections.Generic;

namespace MQT.Events;

public class Order
{
    public DateTime Time { get; set; }
    public string Id { get; set; }
    public Client Client { get; set; }
    public string Address { get; set; }
    public IDictionary<Product, int> ProductQuantities { get; set; }
}
