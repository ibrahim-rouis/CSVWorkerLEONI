using CSVWorker.Exceptions;
using CSVWorker.Libs;
using CSVWorker.Models.Entities;
using CSVWorker.Models.ViewModels.IMDSMacros;
using CSVWorker.Security;
using CSVWorker.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace CSVWorker.Controllers
{
    public class IMDSDatabaseController : Controller
    {
        private readonly ILogger<IMDSDatabaseController> _logger;
        private readonly IMDSDatabaseService _service;

        public IMDSDatabaseController(ILogger<IMDSDatabaseController> logger, IMDSDatabaseService service)
        {
            _logger = logger;
            _service = service;
        }

        public async Task<IActionResult> Index(int? pageNumber, string? query)
        {
            _logger.LogInformation("IMDSDatabase Index page accessed by user {Name} with pageNumber={pageNumber} and query={query}.", User.Identity?.Name, pageNumber, query);

            var model = await _service.GetPagedAsync(pageNumber, query);

            ViewData["query"] = query ?? "";

            return View(model);
        }

        [Authorize(Policy = Policies.AdminOrManagerPolicy)]
        public IActionResult Create()
        {
            _logger.LogInformation("IMDSDatabase Create page accessed by user {Name}.", User.Identity?.Name);
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        [Authorize(Policy = Policies.AdminOrManagerPolicy)]
        public async Task<IActionResult> Create([Bind("PartNumber,ForsPN,SIGIPPN,VisualPN,WGK,NodeID")] IMDSDatabaseRecord model)
        {
            _logger.LogInformation("IMDSDatabase Create attempt by user {Name} with PartNumber={PartNumber}, ForsPN={ForsPN}, SIGIPPN={SIGIPPN}, VisualPN={VisualPN}, WGK={WGK}, NodeID={NodeID}.", User.Identity?.Name, model.PartNumber, model.ForsPN, model.SIGIPPN, model.VisualPN, model.WGK, model.NodeID);
            if (!ModelState.IsValid)
            {
                return View(model);
            }

            try
            {
                await _service.SaveAsync(model, User.Identity?.Name);
                return RedirectToAction(nameof(Index));
            }
            catch (CSVWorkerException ex)
            {
                _logger.LogError(ex, "Error creating IMDSDatabaseRecord");
                ModelState.AddModelError(string.Empty, ex.Message);
                return View(model);
            }
        }

        [Authorize(Policy = Policies.AdminOrManagerPolicy)]
        public async Task<IActionResult> Edit(long id)
        {
            _logger.LogInformation("IMDSDatabase Edit page accessed by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var record = await _service.GetByIdAsync(id);
            if (record == null)
            {
                return NotFound();
            }
            return View(record);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        [Authorize(Policy = Policies.AdminOrManagerPolicy)]
        public async Task<IActionResult> Edit(long id, [Bind("Id,PartNumber,ForsPN,SIGIPPN,VisualPN,WGK,NodeID")] IMDSDatabaseRecord model)
        {
            _logger.LogInformation("IMDSDatabase Edit attempt by user {Name} for record ID={id} with PartNumber={PartNumber}, ForsPN={ForsPN}, SIGIPPN={SIGIPPN}, VisualPN={VisualPN}, WGK={WGK}, NodeID={NodeID}.", User.Identity?.Name, id, model.PartNumber, model.ForsPN, model.SIGIPPN, model.VisualPN, model.WGK, model.NodeID);
            if (id != model.Id)
            {
                return BadRequest();
            }

            if (!ModelState.IsValid)
            {
                return View(model);
            }

            try
            {
                await _service.UpdateAsync(model, User.Identity?.Name);
                return RedirectToAction(nameof(Index));
            }
            catch (CSVWorkerException ex)
            {
                _logger.LogError(ex, "Error updating IMDSDatabaseRecord");
                ModelState.AddModelError(string.Empty, ex.Message);
                return View(model);
            }
        }

        [Authorize(Policy = Policies.AdminOrManagerPolicy)]
        public IActionResult UpdateDatabase()
        {
            _logger.LogInformation("IMDSDatabase UpdateDatabase page accessed by user {Name}.", User.Identity?.Name);
            return View(new UpdateDatabaseVM());
        }

        [HttpPost]
        // The default request size limit in ASP.NET Core is 30 MB, which may not be sufficient for large CSV files.
        // The following attributes increase the limits to 100 MB.
        [RequestSizeLimit(104857600)] // Bump payload limit to 100 MB
        [RequestFormLimits(MultipartBodyLengthLimit = 104857600)] // Bump form upload limit to 100 MB
        [ValidateAntiForgeryToken]
        [Authorize(Policy = Policies.AdminOrManagerPolicy)]
        public async Task<IActionResult> UpdateDatabase(UpdateDatabaseVM model, CancellationToken cancellationToken)
        {
            _logger.LogInformation("IMDSDatabase UpdateDatabase attempt by user {Name} with LPCPFile={LPCPFile}, A2File={A2File}.", User.Identity?.Name, model.LPCPFile?.FileName, model.A2File?.FileName);
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
                await _service.UpdateDatabaseIMDS(model, User.Identity?.Name, cancellationToken);
                model.Success = true;
                model.ErrorMessage = null;
                return View(model);
            }
            catch (CSVWorkerException e)
            {
                model.Success = false;
                model.ErrorMessage = e.Message;
                return View(model);
            }
        }

        // details
        public async Task<IActionResult> Details(long id)
        {
            _logger.LogInformation("IMDSDatabase Details page accessed by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var record = await _service.GetByIdAsync(id);
            if (record == null)
            {
                return NotFound();
            }

            return View(record);
        }

        // delete
        [Authorize(Policy = Policies.AdminOrManagerPolicy)]
        public async Task<IActionResult> Delete(long id)
        {
            _logger.LogInformation("IMDSDatabase Delete page accessed by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var record = await _service.GetByIdAsync(id);
            if (record == null)
            {
                return NotFound();
            }

            return View(record);
        }

        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        [Authorize(Policy = Policies.AdminOrManagerPolicy)]
        public async Task<IActionResult> DeleteConfirmed(long id)
        {
            _logger.LogInformation("IMDSDatabase Delete attempt by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var record = await _service.GetByIdAsync(id);
            if (record == null)
            {
                return NotFound();
            }

            await _service.DeleteAsync(id);

            _logger.LogInformation("IMDSDatabase record ID={id} deleted by user {Name}.", id, User.Identity?.Name);

            return RedirectToAction(nameof(Index));
        }

        // Export
        public async Task<IActionResult> Export(CancellationToken cancellationToken)
        {
            _logger.LogInformation("IMDSDatabase Export attempted by user {Name}.", User.Identity?.Name);

            var outputBytes = await _service.ExportDatabaseAsync(cancellationToken);

            var dateString = DateTime.Now.ToString("yyyy-MM-dd_HHmmss");
            var fileName = $"FORS_IMDS_Database_{dateString}.csv";

            return File(outputBytes, "text/csv", fileName);
        }
    }
}