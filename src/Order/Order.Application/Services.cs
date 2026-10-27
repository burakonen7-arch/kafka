using Order.Domain;
namespace Order.Application;
public sealed record CreateOrder(Guid ProductId, int Quantity);
public interface IOrderService
{
    Task CompleteAsync(Guid orderId, bool success, string? reason, CancellationToken ct);
    Task<OrderItem> CreateAsync(CreateOrder request, CancellationToken ct);
    Task<OrderItem?> GetAsync(Guid id, CancellationToken ct);
    Task<List<OrderItem>> ListAsync(CancellationToken ct);
}
public interface IOrderRepository
{
    Task<OrderItem> GetForUpdateAsync(Guid id, CancellationToken ct);
    Task AddAsync(OrderItem item, CancellationToken ct);
    Task<OrderItem?> GetAsync(Guid id, CancellationToken ct);
    Task<List<OrderItem>> ListAsync(CancellationToken ct);
}
public sealed class OrderService(IOrderRepository repository) : IOrderService
{
   
    public async Task CompleteAsync(Guid orderId, bool success, string? reason, CancellationToken ct)
    {
        var order = await repository.GetForUpdateAsync(orderId, ct);
        if (order.Status != "Pending") return;
        order.Status = success ? "Confirmed" : "Rejected";
        order.FailureReason = reason;
    }
    
    
    public async Task<OrderItem> CreateAsync(CreateOrder request, CancellationToken ct)
    {
        if (request.ProductId == Guid.Empty || request.Quantity <= 0)
            throw new ArgumentException("ProductId and positive Quantity are required.");
       
        var item = new OrderItem { ProductId = request.ProductId, Quantity = request.Quantity };
        await repository.AddAsync(item, ct);
        return item;
    }
    
    
    public Task<OrderItem?> GetAsync(Guid id, CancellationToken ct) => repository.GetAsync(id, ct);
    public Task<List<OrderItem>> ListAsync(CancellationToken ct) => repository.ListAsync(ct);
}
