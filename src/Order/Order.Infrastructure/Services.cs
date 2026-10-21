using Microsoft.EntityFrameworkCore;
using Order.Application;
using Order.Domain;
using Contracts;
using System.Text.Json;
namespace Order.Infrastructure;
public sealed class OrderRepository(AppDbContext db) : IOrderRepository
{
    public Task<OrderItem> GetForUpdateAsync(Guid id, CancellationToken ct) => db.Items.SingleAsync(x => x.Id == id, ct);
    public async Task AddAsync(OrderItem item, CancellationToken ct)
    {
        db.Items.Add(item);
        var message = new OrderCreated(Guid.NewGuid(), DateTimeOffset.UtcNow, item.Id, item.ProductId, item.Quantity);
        db.Outbox.Add(new OutboxMessage { Id = message.EventId, Topic = Topics.OrderCreated, Key = item.Id.ToString(), Payload = JsonSerializer.Serialize(message) });
        await db.SaveChangesAsync(ct); // EF commits entity and outbox atomically.
    }
    public Task<OrderItem?> GetAsync(Guid id, CancellationToken ct) => db.Items.AsNoTracking().SingleOrDefaultAsync(x => x.Id == id, ct);
    public Task<List<OrderItem>> ListAsync(CancellationToken ct) => db.Items.AsNoTracking().OrderBy(x => x.Id).ToListAsync(ct);
}
