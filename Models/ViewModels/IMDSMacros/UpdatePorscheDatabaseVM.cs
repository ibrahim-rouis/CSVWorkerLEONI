using System.ComponentModel.DataAnnotations;

namespace CSVWorker.Models.ViewModels.IMDSMacros
{
    public class UpdatePorscheDatabaseVM
    {
        [Required]
        public IFormFile? PorscheCSV { get; set; }

        public string? ErrorMessage { get; set; }

        public bool Success { get; set; } = false;
    }
}
