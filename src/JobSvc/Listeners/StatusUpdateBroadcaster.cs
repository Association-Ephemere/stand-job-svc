using System.Collections.Concurrent;
using System.Threading.Channels;
using JobSvc.Models;

namespace JobSvc.Listeners;

public sealed class StatusUpdateBroadcaster : BackgroundService
{
    private readonly ChannelReader<StatusUpdate> _source;
    private readonly ConcurrentDictionary<Guid, Channel<StatusUpdate>> _subscribers = new();

    public StatusUpdateBroadcaster(ChannelReader<StatusUpdate> source)
    {
        _source = source;
    }

    public (Guid Id, ChannelReader<StatusUpdate> Reader) Subscribe()
    {
        var id = Guid.NewGuid();
        var channel = Channel.CreateUnbounded<StatusUpdate>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = true
        });
        _subscribers[id] = channel;
        return (id, channel.Reader);
    }

    public void Unsubscribe(Guid id)
    {
        if (_subscribers.TryRemove(id, out var channel))
            channel.Writer.TryComplete();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        try
        {
            await foreach (var update in _source.ReadAllAsync(stoppingToken))
            {
                foreach (var (_, subscriber) in _subscribers)
                {
                    subscriber.Writer.TryWrite(update);
                }
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            foreach (var (id, _) in _subscribers)
                Unsubscribe(id);
        }
    }
}
