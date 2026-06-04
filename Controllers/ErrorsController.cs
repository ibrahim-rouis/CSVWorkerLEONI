using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace CSVWorker.Controllers
{
    [AllowAnonymous]
    public class ErrorsController : Controller
    {
        private readonly ILogger<ErrorsController> _logger;

        public ErrorsController(ILogger<ErrorsController> logger)
        {
            _logger = logger;
        }

        [Route("Errors/{statusCode}")]
        public IActionResult Status(int statusCode)
        {
            // Simply return an empty response for 401s so the Windows Auth browser popup can work
            if (statusCode == 401)
            {
                return Empty;
            }

            _logger.LogWarning("Status page requested: {Status}", statusCode);
            return View(statusCode);
        }

        [Route("Errors/Error")]
        public IActionResult Error()
        {
            Response.StatusCode = 500;
            _logger.LogError("Unhandled exception route hit.");
            return View();
        }
    }
}
