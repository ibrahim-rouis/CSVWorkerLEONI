using System.ComponentModel.DataAnnotations;

namespace CSVWorker.ViewModels.IMDSMacros
{
    public class UpdateDatabaseVM
    {
        [Required]
        public IFormFile? LPCPFile { get; set; }

        [Required]
        public IFormFile? A2File { get; set; }

        public string? ErrorMessage { get; set; }
    }
}
