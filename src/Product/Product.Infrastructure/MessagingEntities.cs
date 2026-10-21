namespace Product.Infrastructure;
public sealed class OutboxMessage
{
    public Guid Id { get; set; }
    public string Topic { get; set; } = "";
    public string Key { get; set; } = "";
    public string Payload { get; set; } = "";
    public DateTimeOffset CreatedAt { get; set; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? PublishedAt { get; set; }
}
public sealed class InboxMessage
{
    public Guid Id { get; set; }
    public DateTimeOffset ProcessedAt { get; set; } = DateTimeOffset.UtcNow;
}
