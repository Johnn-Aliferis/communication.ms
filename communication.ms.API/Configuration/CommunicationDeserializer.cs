using communication.ms.API.DTO;
using Confluent.Kafka;
using System.Text.Json;
using System.Text;
using communication.ms.API.Service;

namespace communication.ms.API.Configuration
{
    public class CommunicationDeserializer : IDeserializer<CommunicationDTO>
    {
        private readonly ILogger<KafkaConsumerService> _logger;

        public CommunicationDTO Deserialize(ReadOnlySpan<byte> data, bool isNull, SerializationContext context)
        {
            if (isNull || data.IsEmpty)
            {
                return null;
            }

            try
            {
                var jsonString = Encoding.UTF8.GetString(data);
                var communicationDTO = JsonSerializer.Deserialize<CommunicationDTO>(jsonString);
                if (communicationDTO == null || !IsValidCommunicationDTO(communicationDTO))
                {
                    throw new JsonException("Invalid message");
                }

                return communicationDTO;
            }
            catch (JsonException e)
            {
                _logger.LogError(e.ToString());
                return null;
            }
        }
        private bool IsValidCommunicationDTO(CommunicationDTO dto)
        {
            return dto.OrderId != null && !string.IsNullOrEmpty(dto.StatusToEmit);
        }
    }
}
