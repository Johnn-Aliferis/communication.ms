using System;
using System.Net;
using System.Net.Mail;

namespace communication.ms.API.Service
{
    public class MailSenderService : IMailSenderService
    {
        private readonly string _smtpServer;
        private readonly int _smtpPort;
        private readonly string _smtpUsername;
        private readonly string _smtpPassword;
        private readonly string _emailSubject;

        public MailSenderService() 
        {
            _smtpServer = Environment.GetEnvironmentVariable("SMTP_SERVER") ?? "smtp.gmail.com";
            _smtpPort = int.TryParse(Environment.GetEnvironmentVariable("SMTP_PORT"), out var port) ? port : 587;
            _smtpUsername = Environment.GetEnvironmentVariable("SMTP_USERNAME") ?? "default_username";
            _smtpPassword = Environment.GetEnvironmentVariable("SMTP_PASSWORD") ?? "default_password";
            _emailSubject = Environment.GetEnvironmentVariable("EMAIL_SUBJECT") ?? "Order Status updated!";
        }  

        public async Task SendEmailAsync(string to, string body)
        {
            // Use of using keyword to ensure client is disposed once its done , even if exception is thrown.
            using var client = new SmtpClient(_smtpServer, _smtpPort);
            client.Credentials = new NetworkCredential(_smtpUsername, _smtpPassword);
            client.EnableSsl = true;

            // Email details.
            var mail = new MailMessage();
            mail.From = new MailAddress(_smtpUsername);
            mail.To.Add(to);
            mail.Subject = _emailSubject;
            mail.Body = body;

            try
            {
                await client.SendMailAsync(mail);
            }
            catch (System.Exception)
            {
                throw new System.Exception();
            }
        }
    }
}
