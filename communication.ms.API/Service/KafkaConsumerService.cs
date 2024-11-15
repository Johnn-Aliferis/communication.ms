
using communication.ms.API.Configuration;
using communication.ms.API.DTO;
using Confluent.Kafka;
using System.Reactive.Linq;

namespace communication.ms.API.Service
{
    public class KafkaConsumerService : IHostedService
    {
        private readonly IConsumer<string, CommunicationDTO> _consumer;
        private readonly ILogger<KafkaConsumerService> _logger;
        private readonly IServiceProvider _serviceProvider;
        private IDisposable? _subscription;
        private readonly int _parrallelMessagesAllowed = 5;

        public KafkaConsumerService(ILogger<KafkaConsumerService> logger, IConfiguration configuration,
            IServiceProvider serviceProvider)
        {
            var consumerConfig = new ConsumerConfig
            {
                BootstrapServers = configuration["Kafka:BootstrapServers"],
                GroupId = configuration["Kafka:GroupId"],
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            _consumer = new ConsumerBuilder<string, CommunicationDTO>(consumerConfig)
                            .SetKeyDeserializer(Deserializers.Utf8)
                            .SetValueDeserializer(new CommunicationDeserializer())
                            .Build();
            _logger = logger;
            _serviceProvider = serviceProvider;
        }

        // Starts during application start-up
        public Task StartAsync(CancellationToken cancellationToken)
        {
            _consumer.Subscribe("notification-topic"); // Should be passed down from enviromental variables.

            _subscription = Observable
                    .Create<ConsumeResult<string, CommunicationDTO>>(observer =>
                         {  
                             var task = Task.Run(() =>
                             {
                                 try
                                 {
                                     while (!cancellationToken.IsCancellationRequested)
                                     {
                                         try
                                         {
                                             var consumeResult = _consumer.Consume(cancellationToken);
                                             if (consumeResult != null)
                                             {
                                                 observer.OnNext(consumeResult);
                                             }
                                         }
                                         catch (ConsumeException ex)
                                         {
                                             _logger.LogError("Kafka consume error: {Message}", ex.Message);
                                         }
                                     }
                                 }
                                 catch (OperationCanceledException)
                                 {
                                     observer.OnCompleted();
                                 }
                             }, cancellationToken);

                             return () => { /* Instead of returning null */ };
                             })
                    .Where(result => result?.Message?.Value != null)
                    .Select(result => Observable.FromAsync(async () =>
                    {
                        await SendEmailTempImplementationAsync(result.Message.Value.StatusToEmit);
                        return result;
                    }))
                    .Merge(_parrallelMessagesAllowed)
                    .Subscribe(
                        _ => _logger.LogInformation("Email sent successfully"), // first for onSuccess
                        ex => _logger.LogError("Error processing message: {Message}", ex.Message) // second for OnError.
                    );
            return Task.CompletedTask;
        }

        private async Task SendEmailTempImplementationAsync(string message)
        {
            await Task.Delay(100);
            _logger.LogInformation("Email sent with status: {Message}", message);
        }


        public Task StopAsync(CancellationToken cancellationToken)
        {
            try
            {
                _consumer.Close();
                _consumer.Dispose();
                _subscription?.Dispose();
            }
            catch (System.Exception e)
            {
                _logger.LogError("Error: {Message}", e.Message);
            }
            return Task.CompletedTask;
        }

        private async Task SendEmail(CommunicationDTO communicationDTO)
        {
            string body = communicationDTO.OrderId + communicationDTO.StatusToEmit;

            // Creating scoped service -- tied to each message processing - Stateful.
            using var scope = _serviceProvider.CreateScope();
            var mailSender = scope.ServiceProvider.GetRequiredService<IMailSenderService>();

            // In production, we should add retry logic to handle transient failures, e.g., if mail service is down
            await mailSender.SendEmailAsync("sender@gmail.com", body);
        }
    }
}
