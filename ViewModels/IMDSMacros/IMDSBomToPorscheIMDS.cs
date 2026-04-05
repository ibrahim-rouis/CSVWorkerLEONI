using System.ComponentModel.DataAnnotations;

namespace CSVWorker.ViewModels.IMDSMacros
{
    public class IMDSBomToPorscheIMDS
    {
        // Database Porsche CSV file, required for Artice Name lookup
        [Required]
        public IFormFile? DatabasePorscheCSV { get; set; }

        [Required]
        public IFormFile? IMDSFileCSV { get; set; }

        public string? ErrorMessage { get; set; }
    }
}
