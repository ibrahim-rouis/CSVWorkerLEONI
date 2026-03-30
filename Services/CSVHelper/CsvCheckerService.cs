namespace CSVWorker.Services.CSVHelper
{
    public class CsvCheckerService : ICsvCheckerService
    {
        public bool IsValidCSV(IFormFile? file)
        {
            if (!IsFile(file))
                return false;

            return string.Equals(Path.GetExtension(file!.FileName), ".csv", StringComparison.OrdinalIgnoreCase);
        }

        public bool IsFile(IFormFile? file)
        {
            return file is not null && file.Length >= 0;
        }
    }
}
