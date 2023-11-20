namespace MQT.Events;

public class Order
{
    public DateTime Time { get; set; }
    public string Id { get; set; }
    public string ClientFirstName { get; set; }
    public string ClientLastName { get; set; }
    public string ClientAddress { get; set; }
    public IDictionary<Product, int> ProductQuantities { get; set; }
}
