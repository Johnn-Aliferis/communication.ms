using System.Text.Json.Serialization;

namespace communication.ms.API.DTO
{
    public class CommunicationDTO
    {
        [JsonPropertyName("orderId")]
        public string OrderId { get; set; }
        [JsonPropertyName("statusToEmit")]
        public string StatusToEmit { get; set; }
    }
}
