using Microsoft.EntityFrameworkCore;
using Product.Application;
using Product.Domain;
using Contracts;
using System.Text.Json;
namespace Product.Infrastructure;
public sealed class ProductRepository(AppDbContext db) : IProductRepository
{
    public async Task<bool> TryDecreaseStockAsync(Guid productId, int quantity, CancellationToken ct) =>
        await db.Items.Where(x => x.Id == productId && x.Stock >= quantity)
            .ExecuteUpdateAsync(s => s.SetProperty(x => x.Stock, x => x.Stock - quantity), ct) == 1;
 
    public async Task AddAsync(ProductItem item, CancellationToken ct)
    {
        db.Items.Add(item);
        await db.SaveChangesAsync(ct);
    }
    
    public Task<ProductItem?> GetAsync(Guid id, CancellationToken ct) => db.Items.AsNoTracking().SingleOrDefaultAsync(x => x.Id == id, ct);
    public Task<List<ProductItem>> ListAsync(CancellationToken ct) => db.Items.AsNoTracking().OrderBy(x => x.Id).ToListAsync(ct);
}
