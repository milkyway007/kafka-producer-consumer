using Confluent.Kafka;
using System;
using System.Threading;
using Microsoft.Extensions.Configuration;

class Consumer
{
    static void Main(string[] args)
    {
        IConfiguration configuration = new ConfigurationBuilder()
            .AddIniFile("C:\\confluent-7.4.0\\confluent-7.4.0\\etc\\kafka\\consumer.properties")
            .Build();

        configuration["group.id"] = "kafka-dotnet-getting-started-2";
        configuration["auto.offset.reset"] = "earliest";

        const string topic = "my-kafka-topic";

        CancellationTokenSource cts = new CancellationTokenSource();
        Console.CancelKeyPress += (_, e) => {
            e.Cancel = true; // prevent the process from terminating.
            cts.Cancel();
        };

        using var consumer = new ConsumerBuilder<string, string>(
            configuration.AsEnumerable()).Build();
        consumer.Subscribe(topic);
        try
        {
            while (true)
            {
                var cr = consumer.Consume(cts.Token);
                Console.WriteLine($"Consumed event from topic {topic} with key {cr.Message.Key,-10} and value {cr.Message.Value}");
            }
        }
        catch (OperationCanceledException)
        {
            // Ctrl-C was pressed.
        }
        finally
        {
            consumer.Close();
        }
    }
}