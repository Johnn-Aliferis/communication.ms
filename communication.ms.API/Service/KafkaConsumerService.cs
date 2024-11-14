
using communication.ms.API.Configuration;
using communication.ms.API.DTO;
using communication.ms.API.Exception;
using Confluent.Kafka;

namespace communication.ms.API.Service
{
    public class KafkaConsumerService : IHostedService
    {
        private readonly IConsumer<string, CommunicationDTO> _consumer;
        private readonly ILogger<KafkaConsumerService> _logger;
        private readonly IServiceProvider _serviceProvider;

        public KafkaConsumerService(ILogger<KafkaConsumerService> logger, IConfiguration configuration, IServiceProvider serviceProvider)
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
        public async Task StartAsync(CancellationToken cancellationToken)
        {
            await Task.Run(() => StartConsumerLoop(cancellationToken), cancellationToken);
        }

        public Task StopAsync(CancellationToken cancellationToken)
        {
            _consumer.Close();
            _consumer.Dispose();
            return Task.CompletedTask;
        }

        private async Task StartConsumerLoop(CancellationToken cancellationToken)
        {
            _consumer.Subscribe("notification-topic"); // Should be passed down from enviromental variables.

            while (!cancellationToken.IsCancellationRequested)
            {
                // start a new task to not handle messages sequentually.Allow parallel processing of messages.
                _ = Task.Run(() => ConsumeAndProcessMessage(cancellationToken), cancellationToken);
            }
        }

        private async Task ConsumeAndProcessMessage(CancellationToken cancellationToken)
        {
            try
            {
                var result = _consumer.Consume(cancellationToken);
                if (result?.Message?.Value != null)
                {
                    await SendEmail(result.Message.Value);
                }
                else
                {
                    throw new InvalidMessageException("Invalid message received from Kafka");
                }
            }
            catch (ConsumeException e)
            {
                _logger.LogError("Error consuming message: {Message}", e.Message);
            }
            catch (InvalidMessageException ex)
            {
                _logger.LogError("Invalid message error: {Message}", ex.Message);
            }
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
