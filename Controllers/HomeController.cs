using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace CSVWorker.Controllers
{
    public class HomeController : Controller
    {
        private readonly ILogger<HomeController> _logger;

        public HomeController(ILogger<HomeController> logger)
        {
            _logger = logger;
        }

        public IActionResult Index()
        {
            return View();
        }

        [AllowAnonymous]
        public IActionResult Error()
        {
            Response.StatusCode = 500;
            _logger.LogError("Unhandled exception route hit.");
            return View("Error");
        }

        [AllowAnonymous]
        public IActionResult Status(int? code)
        {
            var status = code ?? 500;
            Response.StatusCode = status;
            _logger.LogWarning("Status page requested: {Status}", status);
            return View("Status", status); // Views/Home/Status.cshtml
        }
    }
}
