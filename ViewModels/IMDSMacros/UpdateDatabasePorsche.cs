using System.ComponentModel.DataAnnotations;

namespace CSVWorker.ViewModels.IMDSMacros
{
    public class UpdateDatabasePorscheVM
    {
        [Required]
        public IFormFile? LPCPFile { get; set; }

        public string? ErrorMessage { get; set; }
    }
}
