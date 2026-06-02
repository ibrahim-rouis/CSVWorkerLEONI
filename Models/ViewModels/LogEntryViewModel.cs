namespace CSVWorker.Models.ViewModels
{
    public class LogEntryViewModel
    {
        public DateTime Timestamp { get; set; }
        public string Level { get; set; } = string.Empty;
        public string SourceContext { get; set; } = string.Empty;
        public string Message { get; set; } = string.Empty;
        public string? Exception { get; set; }
        public string RawLine { get; set; } = string.Empty;

        public string GetLevelCssClass() => Level.ToUpperInvariant() switch
        {
            "DBG" or "DEBUG" => "log-debug",
            "INF" or "INFORMATION" => "log-info",
            "WRN" or "WARNING" => "log-warning",
            "ERR" or "ERROR" => "log-error",
            _ => "log-info"
        };

        public string GetLevelBorderColor() => Level.ToUpperInvariant() switch
        {
            "DBG" or "DEBUG" => "#36b9cc",
            "INF" or "INFORMATION" => "#1cc88a",
            "WRN" or "WARNING" => "#f6c23e",
            "ERR" or "ERROR" => "#e74a3b",
            _ => "#858796"
        };
    }

    public class LogViewerViewModel
    {
        public DateTime SelectedDate { get; set; } = DateTime.Today;
        public int? SelectedYear { get; set; } = DateTime.Today.Year;
        public int? SelectedMonth { get; set; } = DateTime.Today.Month;
        public string ViewMode { get; set; } = "month"; // "day" or "month"
        public List<LogEntryViewModel> LogEntries { get; set; } = [];
        public List<DateTime> AvailableLogDates { get; set; } = [];
        public string? ErrorMessage { get; set; }
        public string? LevelFilter { get; set; }
        public string? SearchTerm { get; set; }
    }
}