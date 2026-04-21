using CSVWorker.Libs;
using CSVWorker.Services;
using CSVWorker.ViewModels.IMDSMacros;
using Microsoft.AspNetCore.Mvc;

namespace CSVWorker.Controllers
{
    public class IMDSMacrosController : Controller
    {
        private readonly IMDSMacrosService _service;

        public IMDSMacrosController(IMDSMacrosService service)
        {
            _service = service;
        }

        public IActionResult Index()
        {
            return NoContent();
        }
        public IActionResult UpdateDatabaseIMDS()
        {
            return View(new UpdateDatabaseVM());
        }

        [HttpPost]
        // The default request size limit in ASP.NET Core is 30 MB, which may not be sufficient for large CSV files.
        // The following attributes increase the limits to 100 MB.
        [RequestSizeLimit(104857600)] // Bump payload limit to 100 MB
        [RequestFormLimits(MultipartBodyLengthLimit = 104857600)] // Bump form upload limit to 100 MB
        public async Task<IActionResult> UpdateDatabaseIMDS(UpdateDatabaseVM model, CancellationToken cancellationToken)
        {
            if (!ModelState.IsValid)
            {
                model.ErrorMessage = "Please fill all required fields.";
                return View(model);
            }
            if (!CsvHelper.IsValidCSV(model.LPCPFile))
            {
                model.ErrorMessage = "Please select a valid LCPC CSV file.";
                return View(model);
            }
            if (!CsvHelper.IsValidCSV(model.A2File))
            {
                model.ErrorMessage = "Please select a valid A2 CSV file.";
                return View(model);
            }

            try
            {
                var outputBytes = await _service.UpdateDatabaseIMDS(model, cancellationToken);
                // Database name should have date appended to it
                // date should be in for yyyy-mm-dd_HHmmss
                if (outputBytes != null)
                {
                    var dateString = DateTime.Now.ToString("yyyy-MM-dd_HHmmss");
                    var fileName = $"database_{dateString}.csv";
                    return File(outputBytes, "text/csv", fileName);
                }
                else
                {
                    model.ErrorMessage = "An error occurred while processing the files. Please try again.";
                    return View(model);
                }
            }
            catch (Exception e)
            {
                model.ErrorMessage = e.Message;
                return View(model);
            }
        }

        public IActionResult UpdateDatabasePorsche()
        {
            return View(new UpdateDatabasePorscheVM());
        }

        [HttpPost]
        // The default request size limit in ASP.NET Core is 30 MB, which may not be sufficient for large CSV files.
        // The following attributes increase the limits to 100 MB.
        [RequestSizeLimit(104857600)] // Bump payload limit to 100 MB
        [RequestFormLimits(MultipartBodyLengthLimit = 104857600)] // Bump form upload limit to 100 MB
        public async Task<IActionResult> UpdateDatabasePorsche(UpdateDatabasePorscheVM model, CancellationToken cancellationToken)
        {
            if (!ModelState.IsValid)
            {
                model.ErrorMessage = "Please fill all required fields.";
                return View(model);
            }
            if (!CsvHelper.IsValidCSV(model.LPCPFile))
            {
                model.ErrorMessage = "Please select a valid LCPC CSV file.";
                return View(model);
            }

            try
            {
                var outputBytes = await _service.UpdateDatabasePorsche(model, cancellationToken);
                return File(outputBytes, "text/csv", "database_porsche.csv");
            }
            catch (Exception e)
            {
                model.ErrorMessage = e.Message;
                return View(model);
            }
        }

        public IActionResult MultiForsBomToIMDS()
        {
            return View(new MultiForsBomToIMDSBomVM());
        }

        [HttpPost]
        [RequestSizeLimit(104857600)] // Bump payload limit to 100 MB
        [RequestFormLimits(MultipartBodyLengthLimit = 104857600)] // Bump form upload limit to 100 MB
        public async Task<IActionResult> MultiForsBomToIMDS(MultiForsBomToIMDSBomVM model, CancellationToken cancellationToken)
        {
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
                return File(outputBytes, "application/zip", "IMDS_CSV_Files.zip");
            }
            catch (Exception e)
            {
                model.ErrorMessage = e.Message;
                return View(model);
            }
        }

        public IActionResult IMDSBomToPorscheIMDS()
        {
            return View(new IMDSBomToPorscheIMDS());
        }

        [HttpPost]
        [RequestSizeLimit(104857600)] // Bump payload limit to 100 MB
        [RequestFormLimits(MultipartBodyLengthLimit = 104857600)] // Bump form upload limit to 100 MB
        public async Task<IActionResult> IMDSBomToPorscheIMDS(IMDSBomToPorscheIMDS model, CancellationToken cancellationToken)
        {
            if (!ModelState.IsValid)
            {
                model.ErrorMessage = "Please fill all required fields.";
                return View(model);
            }

            if (!CsvHelper.IsValidCSV(model.DatabasePorscheCSV))
            {
                model.ErrorMessage = "Please select a valid Database Porsche CSV file.";
                return View(model);
            }

            if (!CsvHelper.IsValidCSV(model.IMDSFileCSV))
            {
                model.ErrorMessage = "Please select a valid IMDS File CSV.";
                return View(model);
            }

            try
            {
                var (filename, outputBytes) = await _service.IMDSBomToPorscheIMDS(model, cancellationToken);
                return File(outputBytes, "text/csv", filename);
            }
            catch (Exception e)
            {
                model.ErrorMessage = e.Message;
                return View(model);
            }
        }
    }
}
