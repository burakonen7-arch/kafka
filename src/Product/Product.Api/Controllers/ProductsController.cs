using Microsoft.AspNetCore.Mvc;
using Product.Application;
namespace Product.Api.Controllers;
[ApiController]
[Route("api/products")]
public sealed class ProductsController(IProductService service) : ControllerBase
{
    [HttpGet]
    public async Task<IActionResult> List(CancellationToken ct) => Ok(await service.ListAsync(ct));
    [HttpGet("{id:guid}")]
    public async Task<IActionResult> Get(Guid id, CancellationToken ct)
    {
        var item = await service.GetAsync(id, ct);
        return item is null ? NotFound() : Ok(item);
    }
    [HttpPost]
    public async Task<IActionResult> Create(CreateProduct request, CancellationToken ct)
    {
        var item = await service.CreateAsync(request, ct);
        return CreatedAtAction(nameof(Get), new { id = item.Id }, item);
    }
}
