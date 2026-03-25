using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using CSVWorker.Models.ViewModels;
using CSVWorker.Services;

namespace CSVWorker.Controllers
{
    [Authorize(Roles = "Admins")]
    public class LogViewerController : Controller
    {
        private readonly LogViewerService _logViewerService;
        private readonly ILogger<LogViewerController> _logger;

        public LogViewerController(LogViewerService logViewerService, ILogger<LogViewerController> logger)
        {
            _logViewerService = logViewerService;
            _logger = logger;
        }

        public async Task<IActionResult> Index(DateTime? date, int? year, int? month,
            string? viewMode, string? levelFilter, string? searchTerm)
        {
            _logger.LogInformation("LogViewer Index called by user {User} with date={Date}, year={Year}, month={Month}, viewMode={ViewMode}, levelFilter={LevelFilter}, searchTerm={SearchTerm}",
                User.Identity?.Name, date, year, month, viewMode, levelFilter, searchTerm);

            var model = new LogViewerViewModel
            {
                AvailableLogDates = _logViewerService.GetAvailableLogDates(),
                ViewMode = viewMode ?? "month",
                LevelFilter = levelFilter,
                SearchTerm = searchTerm
            };

            try
            {
                if (model.ViewMode == "month" && year.HasValue && month.HasValue)
                {
                    model.SelectedYear = year;
                    model.SelectedMonth = month;
                    model.SelectedDate = new DateTime(year.Value, month.Value, 1);
                    model.LogEntries = await _logViewerService.GetLogsForMonthAsync(
                        year.Value, month.Value, levelFilter, searchTerm);
                }
                else
                {
                    model.SelectedDate = date ?? DateTime.Today;
                    model.LogEntries = await _logViewerService.GetLogsForDateAsync(
                        model.SelectedDate, levelFilter, searchTerm);
                }

                ViewBag.Statistics = _logViewerService.GetLogStatistics(model.LogEntries);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error loading logs");
                model.ErrorMessage = "An error occurred while loading the logs. Please try again.";
            }

            return View(model);
        }

        [HttpGet]
        public async Task<IActionResult> Download(DateTime date)
        {
            _logger.LogInformation("Download logs for date {Date} requested by user {User}", date, User.Identity?.Name);
            var entries = await _logViewerService.GetLogsForDateAsync(date);

            if (entries.Count == 0)
            {
                _logger.LogWarning("No logs found for date: {Date}. Download failed", date);
                return NotFound("No logs found for the specified date.");
            }

            var content = string.Join(Environment.NewLine, entries.Select(e => e.RawLine));
            var fileName = $"CSVWorker-logs-{date:yyyy-MM-dd}.txt";

            return File(System.Text.Encoding.UTF8.GetBytes(content), "text/plain", fileName);
        }

        public async Task<IActionResult> DownloadMonth(int month, int year)
        {
            _logger.LogInformation("Download logs for month {Month}/{Year} requested by user {User}", month, year, User.Identity?.Name);
            var entries = await _logViewerService.GetLogsForMonthAsync(year, month);
            if (entries.Count == 0)
            {
                _logger.LogWarning("No logs found for month: {Month}/{Year}. Download failed", month, year);
                return NotFound("No logs found for the specified month.");
            }
            var content = string.Join(Environment.NewLine, entries.Select(e => e.RawLine));
            var fileName = $"CSVWorker-logs-{year}-{month:00}.txt";
            return File(System.Text.Encoding.UTF8.GetBytes(content), "text/plain", fileName);
        }

        public async Task<IActionResult> ExportAndDownload(DateTime? date, int? year, int? month,
            string? viewMode, string? levelFilter, string? searchTerm)
        {
            _logger.LogInformation("Export and download logs requested by user {User} with date={Date}, year={Year}, month={Month}, viewMode={ViewMode}, levelFilter={LevelFilter}, searchTerm={SearchTerm}",
                User.Identity?.Name, date, year, month, viewMode, levelFilter, searchTerm);

            var model = new LogViewerViewModel
            {
                ViewMode = viewMode ?? "month",
                LevelFilter = levelFilter,
                SearchTerm = searchTerm
            };
            if (model.ViewMode == "month" && year.HasValue && month.HasValue)
            {
                model.SelectedYear = year;
                model.SelectedMonth = month;
                model.SelectedDate = new DateTime(year.Value, month.Value, 1);
                model.LogEntries = await _logViewerService.GetLogsForMonthAsync(
                    year.Value, month.Value, levelFilter, searchTerm);
            }
            else
            {
                model.SelectedDate = date ?? DateTime.Today;
                model.LogEntries = await _logViewerService.GetLogsForDateAsync(
                    model.SelectedDate, levelFilter, searchTerm);
            }
            if (model.LogEntries.Count == 0)
            {
                _logger.LogWarning("No logs found for export. Download failed");
                return NotFound("No logs found for the specified criteria.");
            }
            var content = string.Join(Environment.NewLine, model.LogEntries.Select(e => e.RawLine));
            var fileName = model.ViewMode == "month"
                ? $"CSVWorker-logs-{model.SelectedYear}-{model.SelectedMonth:00}.txt"
                : $"CSVWorker-logs-{model.SelectedDate:yyyy-MM-dd}.txt";
            return File(System.Text.Encoding.UTF8.GetBytes(content), "text/plain", fileName);
        }
    }
}