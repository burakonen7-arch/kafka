using Microsoft.EntityFrameworkCore;
using Product.Domain;
namespace Product.Infrastructure;
public sealed class AppDbContext(DbContextOptions<AppDbContext> options) : DbContext(options)
{
    public DbSet<ProductItem> Items => Set<ProductItem>();
    public DbSet<OutboxMessage> Outbox => Set<OutboxMessage>();
    public DbSet<InboxMessage> Inbox => Set<InboxMessage>();
    protected override void OnModelCreating(ModelBuilder model)
    {
        model.Entity<OutboxMessage>().HasIndex(x => x.PublishedAt);
        model.Entity<ProductItem>().Property(x => x.Price).HasPrecision(18, 2);
        model.Entity<ProductItem>().ToTable(t => t.HasCheckConstraint("CK_Product_Stock", "\"Stock\" >= 0"));
    }
}
