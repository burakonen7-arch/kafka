using Product.Domain;
namespace Product.Application;
public sealed record CreateProduct(string Name, decimal Price, int Stock);
public interface IProductService
{
    Task<bool> ReserveAsync(Guid productId, int quantity, CancellationToken ct);
    Task<ProductItem> CreateAsync(CreateProduct request, CancellationToken ct);
    Task<ProductItem?> GetAsync(Guid id, CancellationToken ct);
    Task<List<ProductItem>> ListAsync(CancellationToken ct);
}
public interface IProductRepository
{
    Task<bool> TryDecreaseStockAsync(Guid productId, int quantity, CancellationToken ct);
    Task AddAsync(ProductItem item, CancellationToken ct);
    Task<ProductItem?> GetAsync(Guid id, CancellationToken ct);
    Task<List<ProductItem>> ListAsync(CancellationToken ct);
}
public sealed class ProductService(IProductRepository repository) : IProductService
{
    public Task<bool> ReserveAsync(Guid productId, int quantity, CancellationToken ct)
    {
        if (quantity <= 0) throw new ArgumentException("Quantity must be positive");
        return repository.TryDecreaseStockAsync(productId, quantity, ct);
    }
    public async Task<ProductItem> CreateAsync(CreateProduct request, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(request.Name) || request.Name.Length > 200 || request.Price <= 0 || request.Stock < 0)
            throw new ArgumentException("Name (1–200 characters), positive price and nonnegative stock are required.");
        var item = new ProductItem { Name = request.Name.Trim(), Price = request.Price, Stock = request.Stock };
        await repository.AddAsync(item, ct);
        return item;
    }
    public Task<ProductItem?> GetAsync(Guid id, CancellationToken ct) => repository.GetAsync(id, ct);
    public Task<List<ProductItem>> ListAsync(CancellationToken ct) => repository.ListAsync(ct);
}
