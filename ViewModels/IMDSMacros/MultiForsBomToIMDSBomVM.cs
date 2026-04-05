using System.ComponentModel.DataAnnotations;

namespace CSVWorker.ViewModels.IMDSMacros
{
    public class MultiForsBomToIMDSBomVM
    {
        // Database CSV file, required for Node ID lookup
        [Required]
        public IFormFile? DatabaseCSV { get; set; }

        // Accepts multiple files
        public IEnumerable<IFormFile> CsvFiles { get; set; } = new List<IFormFile>();

        public string? ErrorMessage { get; set; }
    }
}
