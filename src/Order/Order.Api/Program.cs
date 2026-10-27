using Confluent.Kafka;
using Microsoft.EntityFrameworkCore;
using Order.Application;
using Order.Infrastructure;
var builder = WebApplication.CreateBuilder(args);
var connectionString = builder.Configuration.GetConnectionString("Database");
if (string.IsNullOrWhiteSpace(connectionString))
    throw new InvalidOperationException(
        "ConnectionStrings:Database is required. For local development, start scripts/start-infrastructure.ps1 " +
        "and use the http launch profile (Development). For Docker, use docker compose up -d --build.");
builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen();
builder.Services.AddDbContext<AppDbContext>(o => o.UseNpgsql(connectionString));
builder.Services.AddScoped<IOrderService, OrderService>();
builder.Services.AddScoped<IOrderRepository, OrderRepository>();
var bootstrap = builder.Configuration["Kafka:BootstrapServers"] ?? "localhost:9092";
builder.Services.AddSingleton<IProducer<string, string>>(_ => new ProducerBuilder<string, string>(new ProducerConfig
{ BootstrapServers = bootstrap, EnableIdempotence = true, Acks = Acks.All, MessageTimeoutMs = 10000 }).Build());
builder.Services.AddSingleton(new ConsumerConfig { BootstrapServers = bootstrap, GroupId = "order-service-v1",
    EnableAutoCommit = false, EnableAutoOffsetStore = false, AutoOffsetReset = AutoOffsetReset.Earliest,
    MaxPollIntervalMs = 300000 });
builder.Services.AddHostedService<OutboxWorker>();
builder.Services.AddHostedService<EventConsumer>();
var app = builder.Build();
app.UseExceptionHandler(handler => handler.Run(async context =>
{
    var error = context.Features.Get<Microsoft.AspNetCore.Diagnostics.IExceptionHandlerFeature>()?.Error;
    context.Response.StatusCode = error is ArgumentException ? 400 : 500;
    await Results.Problem(statusCode: context.Response.StatusCode,
        detail: error is ArgumentException ? error.Message : "An unexpected error occurred.").ExecuteAsync(context);
}));
if (app.Environment.IsDevelopment()) { app.UseSwagger(); app.UseSwaggerUI(); }
// Schema creation is opt-in for the disposable local demo; use reviewed migrations in production.
if (builder.Configuration.GetValue<bool>("Database:Initialize"))
{
    using var scope = app.Services.CreateScope();
    await scope.ServiceProvider.GetRequiredService<AppDbContext>().Database.EnsureCreatedAsync();
}
app.MapControllers();
app.Run();
