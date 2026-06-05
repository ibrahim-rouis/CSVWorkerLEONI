namespace CSVWorker.Models.ViewModels.IMDSMacros
{
    public class IMDSBomToPorscheIMDS
    {
        // Accepts multiple files
        public IEnumerable<IFormFile> CsvFiles { get; set; } = new List<IFormFile>();

        public string? ErrorMessage { get; set; }
    }
}
