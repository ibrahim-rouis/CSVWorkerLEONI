using Microsoft.Extensions.Options;
using System.Globalization;
using System.Text.RegularExpressions;
using CSVWorker.Configuration;
using CSVWorker.Models.ViewModels;

namespace CSVWorker.Services
{
    public partial class LogViewerService
    {
        private readonly IWebHostEnvironment _environment;
        private readonly ILogger<LogViewerService> _logger;
        private readonly string _logDirectory;
        private readonly CSVWorkerConfig _config;

        // Regex to parse Serilog log lines with format:
        // {Timestamp:yyyy-MM-dd HH:mm:ss.fff zzz} [{Level:u3}] [{SourceContext}] {Message:lj}
        [GeneratedRegex(@"^(?<timestamp>\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}\.\d{3}\s+[+-]\d{2}:\d{2})\s+\[(?<level>\w+)\]\s+\[(?<context>[^\]]*)\]\s+(?<message>.*)$", RegexOptions.Compiled)]
        private static partial Regex LogLineRegex();

        public LogViewerService(IWebHostEnvironment environment, ILogger<LogViewerService> logger, IOptions<CSVWorkerConfig> config)
        {
            _environment = environment;
            _logger = logger;
            _logDirectory = Path.Combine(_environment.ContentRootPath, "serilog");
            _config = config.Value;
        }

        public List<DateTime> GetAvailableLogDates()
        {
            var dates = new List<DateTime>();

            if (!Directory.Exists(_logDirectory))
            {
                _logger.LogWarning("Log directory does not exist: {LogDirectory}", _logDirectory);
                return dates;
            }

            // Log files are named: CSVWorker-YYYYMMDD.log
            var logFiles = Directory.GetFiles(_logDirectory, "CSVWorker-*.log").Order();
            int count = 0;
            foreach (var file in logFiles)
            {
                var fileName = Path.GetFileNameWithoutExtension(file);
                // Extract date from filename: CSVWorker-20260218
                var dateStr = fileName.Replace("CSVWorker-", "");

                if (DateTime.TryParseExact(dateStr, "yyyyMMdd", CultureInfo.InvariantCulture,
                    DateTimeStyles.None, out var date))
                {
                    dates.Add(date);
                }
                count++;
                // Limit the number of dates returned to avoid overwhelming the UI if there are many log files
                if (count >= _config.DefaultLogsHistoryCount)
                    break;
            }

            return dates.OrderByDescending(d => d).ToList();
        }

        public async Task<List<LogEntryViewModel>> GetLogsForDateAsync(DateTime date,
            string? levelFilter = null, string? searchTerm = null)
        {
            var entries = new List<LogEntryViewModel>();
            var logFilePath = GetLogFilePath(date);

            if (!File.Exists(logFilePath))
            {
                // _logger.LogInformation("Log file does not exist for date {Date}: {Path}", date, logFilePath);
                return entries;
            }

            try
            {
                // Use FileShare.ReadWrite to allow reading while Serilog is writing
                await using var fileStream = new FileStream(logFilePath, FileMode.Open,
                    FileAccess.Read, FileShare.ReadWrite);
                using var reader = new StreamReader(fileStream);

                string? line;
                LogEntryViewModel? currentEntry = null;

                while ((line = await reader.ReadLineAsync()) != null)
                {
                    if (string.IsNullOrWhiteSpace(line))
                        continue;

                    var match = LogLineRegex().Match(line);
                    if (match.Success)
                    {
                        // Save previous entry if exists
                        if (currentEntry != null)
                        {
                            entries.Add(currentEntry);
                        }

                        currentEntry = new LogEntryViewModel
                        {
                            RawLine = line,
                            Level = match.Groups["level"].Value,
                            SourceContext = match.Groups["context"].Value,
                            Message = match.Groups["message"].Value
                        };

                        if (DateTimeOffset.TryParse(match.Groups["timestamp"].Value, out var timestamp))
                        {
                            currentEntry.Timestamp = timestamp.LocalDateTime;
                        }
                    }
                    else if (currentEntry != null)
                    {
                        // This is a continuation line (likely an exception stack trace)
                        currentEntry.Exception = string.IsNullOrEmpty(currentEntry.Exception)
                            ? line
                            : currentEntry.Exception + Environment.NewLine + line;
                        currentEntry.RawLine += Environment.NewLine + line;
                    }
                }

                // Add the last entry
                if (currentEntry != null)
                {
                    entries.Add(currentEntry);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error reading log file: {Path}", logFilePath);
                throw;
            }

            // Apply filters
            if (!string.IsNullOrEmpty(levelFilter))
            {
                entries = entries.Where(e =>
                    e.Level.Equals(levelFilter, StringComparison.OrdinalIgnoreCase)).ToList();
            }

            if (!string.IsNullOrEmpty(searchTerm))
            {
                entries = entries.Where(e =>
                    e.Message.Contains(searchTerm, StringComparison.OrdinalIgnoreCase) ||
                    e.SourceContext.Contains(searchTerm, StringComparison.OrdinalIgnoreCase) ||
                    (e.Exception?.Contains(searchTerm, StringComparison.OrdinalIgnoreCase) ?? false)
                ).ToList();
            }

            return entries;
        }

        public async Task<List<LogEntryViewModel>> GetLogsForMonthAsync(int year, int month,
            string? levelFilter = null, string? searchTerm = null)
        {
            var allEntries = new List<LogEntryViewModel>();
            var daysInMonth = DateTime.DaysInMonth(year, month);

            for (int day = 1; day <= daysInMonth; day++)
            {
                var date = new DateTime(year, month, day);
                var entries = await GetLogsForDateAsync(date, levelFilter, searchTerm);
                allEntries.AddRange(entries);
            }

            return allEntries.OrderByDescending(e => e.Timestamp).ToList();
        }

        private string GetLogFilePath(DateTime date)
        {
            var fileName = $"CSVWorker-{date:yyyyMMdd}.log";
            return Path.Combine(_logDirectory, fileName);
        }

        public Dictionary<string, int> GetLogStatistics(List<LogEntryViewModel> entries)
        {
            return entries
                .GroupBy(e => e.Level.ToUpperInvariant())
                .ToDictionary(g => g.Key, g => g.Count());
        }
    }
}