using CSVWorker.Exceptions;
using CSVWorker.Libs;
using CSVWorker.Models.ViewModels.IMDSMacros;
using CSVWorker.Services;
using Microsoft.AspNetCore.Mvc;

namespace CSVWorker.Controllers
{
    public class IMDSMacrosController : Controller
    {
        private readonly IMDSMacrosService _service;

        private readonly ILogger<IMDSMacrosController> _logger;

        public IMDSMacrosController(IMDSMacrosService service, ILogger<IMDSMacrosController> logger)
        {
            _service = service;
            _logger = logger;
        }

        public IActionResult Index()
        {
            return NoContent();
        }

        public IActionResult MultiForsBomToIMDS()
        {
            _logger.LogInformation("MultiForsBomToIMDS page accessed by user {Name}.", User.Identity?.Name);
            return View(new MultiForsBomToIMDSBomVM());
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> MultiForsBomToIMDS(MultiForsBomToIMDSBomVM model, CancellationToken cancellationToken)
        {
            _logger.LogInformation("POST action sent by user {Name} to MultiForsBomToIMDS.", User.Identity?.Name);
            if (!ModelState.IsValid)
            {
                model.ErrorMessage = "Please fill all required fields.";
                return View(model);
            }

            foreach (var csvFile in model.CsvFiles)
            {
                if (!CsvHelper.IsValidCSV(csvFile))
                {
                    model.ErrorMessage = $"You have uploaded an invalid csv file named {csvFile.FileName}. Please make sure all uploaded files are valid CSVs.";
                    return View(model);
                }
            }

            try
            {
                var outputBytes = await _service.MultiForsBomToIMDS(model, cancellationToken);
                if (outputBytes == null)
                {
                    model.ErrorMessage = "An error occurred while processing the files. Please try again.";
                    return View(model);
                }

                var dateString = DateTime.Now.ToString("yyyy-MM-dd_HHmmss");
                var fileName = $"IMDS_CSV_Files_{dateString}.zip";
                Response.Cookies.Append("fileDownloadToken", "success", new CookieOptions { Path = "/", HttpOnly = false, Secure = false });
                return File(outputBytes, "application/zip", fileName);
            }
            catch (CSVWorkerException e)
            {
                model.ErrorMessage = e.Message;
                return View(model);
            }
        }

        public IActionResult IMDSBomToPorscheIMDS()
        {
            _logger.LogInformation("IMDSBomToPorscheIMDS page accessed by user {Name}.", User.Identity?.Name);
            return View(new IMDSBomToPorscheIMDS());
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> IMDSBomToPorscheIMDS(IMDSBomToPorscheIMDS model, CancellationToken cancellationToken)
        {
            _logger.LogInformation("POST action sent by user {Name} to IMDSBomToPorscheIMDS.", User.Identity?.Name);

            if (!ModelState.IsValid)
            {
                model.ErrorMessage = "Please fill all required fields.";
                return View(model);
            }

            foreach (var csvFile in model.CsvFiles)
            {
                if (!CsvHelper.IsValidCSV(csvFile))
                {
                    model.ErrorMessage = $"You have uploaded an invalid csv file named {csvFile.FileName}. Please make sure all uploaded files are valid CSVs.";
                    return View(model);
                }
            }

            try
            {
                var outputBytes = await _service.IMDSBomToPorscheIMDS(model, cancellationToken);
                if (outputBytes == null)
                {
                    model.ErrorMessage = "An error occurred while processing the files. Please try again.";
                    return View(model);
                }

                var dateString = DateTime.Now.ToString("yyyy-MM-dd_HHmmss");
                var fileName = $"Porsche_IMDS_CSV_Files_{dateString}.zip";
                Response.Cookies.Append("fileDownloadToken", "success", new CookieOptions { Path = "/", HttpOnly = false, Secure = false });
                return File(outputBytes, "application/zip", fileName);
            }
            catch (CSVWorkerException e)
            {
                model.ErrorMessage = e.Message;
                return View(model);
            }
        }
    }
}
