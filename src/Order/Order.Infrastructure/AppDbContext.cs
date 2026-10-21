using Microsoft.EntityFrameworkCore;
using Order.Domain;
namespace Order.Infrastructure;
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<OrderItem> Items => Set<OrderItem>();
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();
    protected override void OnModelCreating(ModelBuilder model)
    {
        model.Entity<OutboxMessage>().HasIndex(x => x.PublishedAt);
    }
}
