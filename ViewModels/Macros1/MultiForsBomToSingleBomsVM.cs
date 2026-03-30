namespace CSVWorker.ViewModels.Macros1
{
    public class MultiForsBomToSingleBomsVM
    {
        // Accepts multiple files from the folder
        public IEnumerable<IFormFile> CsvFiles { get; set; } = new List<IFormFile>();

        public string? ErrorMessage { get; set; }
    }
}
