using System.ComponentModel.DataAnnotations;

namespace CSVWorker.ViewModels.IMDSMacros
{
    public class IMDSBomToPorscheIMDS
    {
        // Database Porsche CSV file, required for Artice Name lookup
        [Required]
        public IFormFile? DatabasePorscheCSV { get; set; }

        // Accepts multiple files
        public IEnumerable<IFormFile> CsvFiles { get; set; } = new List<IFormFile>();

        public string? ErrorMessage { get; set; }
    }
}
