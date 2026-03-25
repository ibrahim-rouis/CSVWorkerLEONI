namespace CSVWorker.Configuration
{
    // This class takes values from appsettings.json and makes them available for injection into services and controllers
    // These values intialized here are just in case appsettings.json is missing or has invalid values, so the application can still run with some default settings
    public class CSVWorkerConfig
    {
        public int DefaultPageSize { get; set; } = 5;
        public int DefaultLogsHistoryCount { get; set; } = 30;
    }
}
