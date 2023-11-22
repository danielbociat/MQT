namespace MQT.Events;

public class Product
{
    public string Name { get; set; }
    public string Id{ get; set; }

    public override int GetHashCode()
    {
        return Id.GetHashCode();
    }
    public override bool Equals(object obj)
    {
        return Equals(obj as Product);
    }
    public bool Equals(Product obj)
    {
        return obj != null && obj.Id == this.Id;
    }
}
