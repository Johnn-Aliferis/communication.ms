using Microsoft.AspNetCore.Mvc;

namespace communication.ms.API.Controllers
{
    [Route("api")]
    [ApiController]
    public class CommunicationController : ControllerBase
    {
        [HttpGet("hello")]
        public ActionResult GetHello()
        {
            return Ok("Hello World");
        }
    }
}
