using Order.Application;
using Confluent.Kafka;
using Contracts;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;
using Order.Domain;
namespace Order.Infrastructure;

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
                
                
                //burada henüz kafka'ya gönderilmemiş, outbox tablosunda kayıtlı 50 kaydı,
                //kafkaya göndermek için alırız.
                var batch = await db.Outbox
                    .Where(x => x.PublishedAt == null) //henüz kaydedilmiş mesajlar için // henüz gönderildi olarak işaretlenmedi
                    .OrderBy(x => x.CreatedAt).Take(50)
                    .ToListAsync(ct);
                
                //outbox'dan gelen mesajlar tek tek publish edilir
                foreach (var row in batch)
                {
                    await producer.ProduceAsync(
                        row.Topic, 
                        new Message<string, string>
                        {
                            Key = row.Key, 
                            Value = row.Payload
                            
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
        consumer.Subscribe(Topics.StockReserved); //abone oluyoruz
        try
        {
            while (!ct.IsCancellationRequested)
            {
                var record = consumer.Consume(ct); //mesajları almaya başlıyoruz

                while (!ct.IsCancellationRequested)
                {
                    try
                    {
                        using var scope = scopes.CreateScope();
                        var db = scope.ServiceProvider.GetRequiredService<AppDbContext>();
                        
                        var message = JsonSerializer.Deserialize<StockReserved>(record.Message.Value)
                            ?? throw new InvalidOperationException("Empty event");
                        
                        if (message.EventId == Guid.Empty || message.OrderId == Guid.Empty)
                            throw new InvalidOperationException("Invalid event contract; inspect producer before retrying");
                        
                        
                        //bütün işlemlerin tek bir bütün olarak kalıcı bi şekilde tamamlanması için
                        //transaction oluşturulur
                        await using var tx = await db.Database.BeginTransactionAsync(ct);
                        
                        
                        //gelen mesajın inbox kontrolü sağlanır
                        if (!await db.Inbox.AnyAsync(x => x.Id == message.EventId, ct))
                        {
                            await scope.ServiceProvider
                                .GetRequiredService<IOrderService>()
                                .CompleteAsync(message.OrderId, message.Success, message.Reason, ct);
                            
                            db.Inbox.Add(new InboxMessage { Id = message.EventId });
                            await db.SaveChangesAsync(ct);
                        }
                        
                        //transaction kalıcı hale getirilir
                        await tx.CommitAsync(ct);
                        consumer.Commit(record); 
                        
                        logger.LogInformation(
                            "Processed {Topic} partition {Partition} offset {Offset}", record.Topic, 
                            record.Partition.Value, record.Offset.Value);
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
