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
    [Authorize(Roles = Roles.AdminOrMaterialCompliance)]
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
            _logger.LogInformation($"IMDSDatabase Index page accessed by user {User.Identity?.Name} with pageNumber={pageNumber} and query={query}.");

            var model = await _service.GetPagedAsync(pageNumber, query);

            ViewData["query"] = query ?? "";

            return View(model);
        }

        public IActionResult Create()
        {
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("PartNumber,ForsPN,SIGIPPN,VisualPN,WGK,NodeID")] IMDSDatabaseRecord model)
        {
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

        public async Task<IActionResult> Edit(int id)
        {
            var record = await _service.GetByIdAsync(id);
            if (record == null)
            {
                return NotFound();
            }
            return View(record);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("Id,PartNumber,ForsPN,SIGIPPN,VisualPN,WGK,NodeID")] IMDSDatabaseRecord model)
        {
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

        public IActionResult UpdateDatabase()
        {
            return View(new UpdateDatabaseVM());
        }

        [HttpPost]
        // The default request size limit in ASP.NET Core is 30 MB, which may not be sufficient for large CSV files.
        // The following attributes increase the limits to 100 MB.
        [RequestSizeLimit(104857600)] // Bump payload limit to 100 MB
        [RequestFormLimits(MultipartBodyLengthLimit = 104857600)] // Bump form upload limit to 100 MB
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> UpdateDatabase(UpdateDatabaseVM model, CancellationToken cancellationToken)
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
    }
}
