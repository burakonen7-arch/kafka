using Product.Application;
using Confluent.Kafka;
using Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;
namespace Product.Infrastructure;

public sealed class OutboxWorker(IServiceScopeFactory scopes, IProducer<string, string> producer, ILogger<OutboxWorker> logger) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken ct)
    {
        while (!ct.IsCancellationRequested)
        {
            try
            {
                using var scope = scopes.CreateScope();
                var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                
                var batch = await db.Outbox
                    .Where(x => x.PublishedAt == null)
                    .OrderBy(x => x.CreatedAt)
                    .Take(50)
                    .ToListAsync(ct);
                
                foreach (var row in batch)
                {
                    await producer.ProduceAsync(
                        row.Topic, 
                        new Message<string, string>
                        {
                            Key = row.Key, Value = row.Payload
                        }, ct);
                    
                    row.PublishedAt = DateTimeOffset.UtcNow;
                    await db.SaveChangesAsync(ct);
                }
            }
            
            catch (OperationCanceledException) when (ct.IsCancellationRequested) { break; }
            catch (Exception ex) { logger.LogError(ex, "Outbox publish failed; pending rows will be retried"); }
            await Task.Delay(1000, ct);
        }
    }
}
public sealed class EventConsumer(IServiceScopeFactory scopes, ConsumerConfig config, ILogger<EventConsumer> logger) : BackgroundService
{
    protected override Task ExecuteAsync(CancellationToken ct) => Task.Run(async () =>
    {
        using var consumer = new ConsumerBuilder<string, string>(config).Build();
        consumer.Subscribe(Topics.OrderCreated); // consumer'ın topic'e abone olmasını sağlıyoruz
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var record = consumer.Consume(ct); // burada ise consumer artık mesajları almaya başlıyor
                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        using var scope = scopes.CreateScope();
                        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                        
                        var message = JsonSerializer.Deserialize<OrderCreated>(record.Message.Value)
                            ?? throw new InvalidOperationException("Empty event");
                        
                        if (message.EventId == Guid.Empty ||
                            message.OrderId == Guid.Empty || 
                            message.ProductId == Guid.Empty ||
                            message.Quantity <= 0)
                            throw new InvalidOperationException("Invalid event contract; inspect producer before retrying");
                        
                        //burada 3 farklı işlem gerçekleşeceği için
                        //transaction oluşturulması lazım çünkü bu 3 işlemin kalıcılığı gerekli.
                        //işlem 1i hatalı olursa, diğerlerininde geri alınabilmesi için transaction oluşuyor.
                        await using var tx = await db.Database.BeginTransactionAsync(ct);
                        
                        //gelen mesajın inbox tablosundaki durumu kontrol edilir
                        if (!await db.Inbox.AnyAsync(x => x.Id == message.EventId, ct))
                        {
                            var reserved = await scope.ServiceProvider
                                .GetRequiredService<IProductService>()
                                .ReserveAsync(message.ProductId, message.Quantity, ct);
                            
                            var result = new StockReserved(
                                Guid.NewGuid(), 
                                DateTimeOffset.UtcNow, 
                                message.OrderId, 
                                reserved,
                                reserved ? null : "Product does not exist or stock is insufficient");
                            
                            //burada aynı zamanda işlemin sonucu outbox tablosuna yazılır,
                            db.Outbox.Add(new OutboxMessage {
                                Id = result.EventId, 
                                Topic = Topics.StockReserved,
                                Key = message.OrderId.ToString(), 
                                Payload = JsonSerializer.Serialize(result) 
                            });
                            
                            //mesajın tekrardan işlenip işlenmediğinin kontrolü için
                            //mesajı inbox tablosuna kaydediyoruz.
                            //böylelik aynı mesaj tekrar gelirse,
                            //işlenip işlenmediği bu şekilde kontrol edilir
                            db.Inbox.Add(new InboxMessage { Id = message.EventId });
                            await db.SaveChangesAsync(ct);
                        }
                        
                        //transaction kalıcı hale getirilir
                        await tx.CommitAsync(ct);
                        consumer.Commit(record); 
                        logger.LogInformation(
                            "Processed {Topic} partition {Partition} offset {Offset}",
                            record.Topic, record.Partition.Value, record.Offset.Value);
                        break;
                    }
                    
                    catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
                    catch (Exception ex)
                    {
                        logger.LogError(
                            ex, 
                            "Event failed at {Topic}/{Offset}; retrying, offset remains uncommitted", 
                            record.Topic, 
                            record.Offset);
                        
                        await Task.Delay(2000, ct);
                    }
                }
            }
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { }
        finally { consumer.Close(); }
    }, ct);
}
