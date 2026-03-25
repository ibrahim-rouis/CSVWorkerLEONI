using System.ComponentModel.DataAnnotations;

namespace CSVWorker.Models.ViewModels
{
    public class LoginViewModel
    {
        [Required]
        [Display(Name = "Windows Username")]
        public string? Username { get; set; }

        [Required]
        [DataType(DataType.Password)]
        public string? Password { get; set; }

        public string? ReturnUrl { get; set; }
    }
}
