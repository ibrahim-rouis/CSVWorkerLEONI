using System.Diagnostics;
using Microsoft.AspNetCore.Mvc;
using CSVWorker.Models.ViewModels;

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
            _logger.LogInformation("User {User} Accessing Home/Index", User.Identity?.Name);
            return View();
        }

        public IActionResult Privacy()
        {
            _logger.LogInformation("User {User} Accessing Home/Privacy", User.Identity?.Name);
            return View();
        }

        [ResponseCache(Duration = 0, Location = ResponseCacheLocation.None, NoStore = true)]
        public IActionResult Error()
        {
            return View(new ErrorViewModel { RequestId = Activity.Current?.Id ?? HttpContext.TraceIdentifier });
        }
    }
}
