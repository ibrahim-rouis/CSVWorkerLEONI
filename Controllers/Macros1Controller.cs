using CSVWorker.Services.CSVHelper;
using CSVWorker.Services.Macros;
using CSVWorker.ViewModels.Macros1;
using Microsoft.AspNetCore.Mvc;

namespace CSVWorker.Controllers
{
    public class Macros1Controller : Controller
    {
        private readonly Macros1Service _service;
        private readonly ICsvCheckerService _checker;

        public Macros1Controller(Macros1Service service, ICsvCheckerService checker, ILogger<Macros1Controller> logger)
        {
            _checker = checker;
            _service = service;
        }

        public IActionResult Index()
        {
            return NoContent();
        }
        public IActionResult UpdateDatabase()
        {
            return View(new UpdateDatabaseVM());
        }

        [HttpPost]
        [RequestSizeLimit(104857600)] // Bump payload limit to 100 MB
        [RequestFormLimits(MultipartBodyLengthLimit = 104857600)] // Bump form upload limit to 100 MB
        public async Task<IActionResult> UpdateDatabase(UpdateDatabaseVM model, CancellationToken cancellationToken)
        {
            if (!ModelState.IsValid)
            {
                model.ErrorMessage = "Please fill all required fields.";
                return View(model);
            }
            if (!_checker.IsValidCSV(model.LPCPFile))
            {
                model.ErrorMessage = "Please select a valid LCPC CSV file.";
                return View(model);
            }
            if (!_checker.IsValidCSV(model.A2File))
            {
                model.ErrorMessage = "Please select a valid A2 CSV file.";
                return View(model);
            }

            try
            {
                var outputBytes = await _service.UpdateDatabase(model, cancellationToken);
                return File(outputBytes, "text/csv", "database.csv");
            }
            catch (Exception e)
            {
                model.ErrorMessage = e.ToString();
                return View(model);
            }
        }

        public IActionResult MultiForsBomToSingleBoms()
        {
            return View(new MultiForsBomToSingleBomsVM());
        }

        [HttpPost]
        public async Task<IActionResult> MultiForsBomToSingleBoms(MultiForsBomToSingleBomsVM model, CancellationToken cancellationToken)
        {
            if (!ModelState.IsValid)
            {
                model.ErrorMessage = "Please fill all required fields.";
                return View(model);
            }

            foreach (var csvFile in model.CsvFiles)
            {
                if (!_checker.IsValidCSV(csvFile))
                {
                    model.ErrorMessage = "You have uploaded an invalid csv file";
                    return View(model);
                }
            }

            try
            {
                //var outputStream = await _service.MultiForsBomToSingleBoms(model, cancellationToken);
                //return File(outputStream, "application/zip", "IMDS_CSV_Files.zip");
                return View(model);
            }
            catch (Exception e)
            {
                model.ErrorMessage = e.ToString();
                return View(model);
            }
        }
    }
}
