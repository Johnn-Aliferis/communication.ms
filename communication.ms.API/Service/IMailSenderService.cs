namespace communication.ms.API.Service
{
    public interface IMailSenderService
    {
        Task SendEmailAsync(string to, string body);
    }
}
