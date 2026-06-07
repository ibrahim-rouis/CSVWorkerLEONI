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
    public class IMDSPorscheDatabaseController : Controller
    {
        private readonly ILogger<IMDSPorscheDatabaseController> _logger;
        private readonly IMDSPorscheDatabaseService _service;

        public IMDSPorscheDatabaseController(ILogger<IMDSPorscheDatabaseController> logger, IMDSPorscheDatabaseService service)
        {
            _logger = logger;
            _service = service;
        }

        public async Task<IActionResult> Index(int? pageNumber, string? query)
        {
            _logger.LogInformation("IMDSPorscheDatabase Index page accessed by user {Name} with pageNumber={pageNumber} and query={query}.", User.Identity?.Name, pageNumber, query);
            var model = await _service.GetPagedAsync(pageNumber, query);

            ViewData["query"] = query ?? "";

            return View(model);
        }

        [Authorize(Policy = "AdminOrManager")]
        public IActionResult Create()
        {
            _logger.LogInformation("IMDSPorscheDatabase Create page accessed by user {Name}.", User.Identity?.Name);
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        [Authorize(Policy = "AdminOrManager")]
        public async Task<IActionResult> Create([Bind("PartNumber,ArticleName,MaterialGroup,CrossSec")] IMDSPorscheDatabaseRecord model)
        {
            _logger.LogInformation("IMDSPorscheDatabase Create attempt by user {Name} with PartNumber={PartNumber}, ArticleName={ArticleName}, MaterialGroup={MaterialGroup}, CrossSec={CrossSec}.", User.Identity?.Name, model.PartNumber, model.ArticleName, model.MaterialGroup, model.CrossSec);
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
                _logger.LogError(ex, "Error creating IMDSPorscheDatabaseRecord");
                ModelState.AddModelError(string.Empty, ex.Message);
                return View(model);
            }
        }

        [Authorize(Policy = "AdminOrManager")]
        public async Task<IActionResult> Edit(int id)
        {
            _logger.LogInformation("IMDSPorscheDatabase Edit page accessed by user {Name} for record ID {id}.", User.Identity?.Name, id);
            var record = await _service.GetByIdAsync(id);
            if (record == null)
            {
                return NotFound();
            }
            return View(record);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        [Authorize(Policy = "AdminOrManager")]
        public async Task<IActionResult> Edit(long id, [Bind("Id,PartNumber,ArticleName,MaterialGroup,CrossSec")] IMDSPorscheDatabaseRecord model)
        {
            _logger.LogInformation("IMDSPorscheDatabase Edit attempt by user {Name} for record ID={id} with PartNumber={PartNumber}, ArticleName={ArticleName}, MaterialGroup={MaterialGroup}, CrossSec={CrossSec}.", User.Identity?.Name, id, model.PartNumber, model.ArticleName, model.MaterialGroup, model.CrossSec);
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
                _logger.LogError(ex, "Error updating IMDSPorscheDatabaseRecord");
                ModelState.AddModelError(string.Empty, ex.Message);
                return View(model);
            }
        }

        [Authorize(Policy = "AdminOrManager")]
        public IActionResult UpdatePorscheDatabase()
        {
            return View(new UpdatePorscheDatabaseVM());
        }

        [HttpPost]
        // The default request size limit in ASP.NET Core is 30 MB, which may not be sufficient for large CSV files.
        // The following attributes increase the limits to 100 MB.
        [RequestSizeLimit(104857600)] // Bump payload limit to 100 MB
        [RequestFormLimits(MultipartBodyLengthLimit = 104857600)] // Bump form upload limit to 100 MB
        [ValidateAntiForgeryToken]
        [Authorize(Policy = "AdminOrManager")]
        public async Task<IActionResult> UpdatePorscheDatabase(UpdatePorscheDatabaseVM model, CancellationToken cancellationToken)
        {
            _logger.LogInformation("POST action sent by user {Name} to UpdatePorscheDatabase with file {FileName}.", User.Identity?.Name, model.PorscheCSV?.FileName);
            if (!ModelState.IsValid)
            {
                model.ErrorMessage = "Please fill all required fields.";
                return View(model);
            }
            if (!CsvHelper.IsValidCSV(model.PorscheCSV))
            {
                model.ErrorMessage = "Please select a valid Porsche CSV file.";
                return View(model);
            }

            try
            {
                await _service.UpdatePorscheDatabase(model, User.Identity?.Name, cancellationToken);
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
            _logger.LogInformation("IMDSPorscheDatabase Details page accessed by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var record = await _service.GetByIdAsync(id);
            if (record == null)
            {
                return NotFound();
            }

            return View(record);
        }

        // delete
        [Authorize(Policy = "AdminOrManager")]
        public async Task<IActionResult> Delete(long id)
        {
            _logger.LogInformation("IMDSPorscheDatabase Delete page accessed by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var record = await _service.GetByIdAsync(id);
            if (record == null)
            {
                return NotFound();
            }

            return View(record);
        }

        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        [Authorize(Policy = "AdminOrManager")]
        public async Task<IActionResult> DeleteConfirmed(long id)
        {
            _logger.LogInformation("IMDSPorscheDatabase Delete attempt by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var record = await _service.GetByIdAsync(id);
            if (record == null)
            {
                return NotFound();
            }

            await _service.DeleteAsync(id);

            _logger.LogInformation("IMDSPorscheDatabase record ID={id} deleted by user {Name}.", id, User.Identity?.Name);

            return RedirectToAction(nameof(Index));
        }
    }
}
