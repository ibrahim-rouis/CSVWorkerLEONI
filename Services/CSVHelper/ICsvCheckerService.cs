namespace CSVWorker.Services.CSVHelper
{
    public interface ICsvCheckerService
    {
        bool IsValidCSV(IFormFile? file);
        protected bool IsFile(IFormFile? file);
    }
}
