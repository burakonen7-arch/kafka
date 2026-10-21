namespace Contracts;
public static class Topics
{
    public const string OrderCreated = "orders.created.v1";
    public const string StockReserved = "products.stock-reserved.v1";
}
public sealed record OrderCreated(Guid EventId, DateTimeOffset OccurredAt, Guid OrderId, Guid ProductId, int Quantity);
public sealed record StockReserved(Guid EventId, DateTimeOffset OccurredAt, Guid OrderId, bool Success, string? Reason);
