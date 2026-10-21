namespace Order.Domain;
public sealed class OrderItem
{
    public Guid Id { get; set; } = Guid.NewGuid();
    public Guid ProductId { get; set; }
    public int Quantity { get; set; }
    public string Status { get; set; } = "Pending";
    public string? FailureReason { get; set; }
}
