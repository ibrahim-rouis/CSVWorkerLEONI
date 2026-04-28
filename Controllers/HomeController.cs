using CSVWorker.ViewModels;
using Microsoft.AspNetCore.Mvc;
using System.Diagnostics;

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

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }

        [Route("NotFound")]
        public IActionResult NotFoundPage()
        {
            // It's good practice to set the status code explicitly on the response
            Response.StatusCode = 404;
            return View("NotFound");
        }
    }
}
