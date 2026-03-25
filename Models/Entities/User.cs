using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;

namespace CSVWorker.Models.Entities
{
    // Set a unique index on the Name property
    [Index(nameof(Name), IsUnique = true)]
    public class User
    {
        public int Id { get; set; }

        [Required]
        [StringLength(255)]
        public string? Name { get; set; }

        public List<Role>? Roles { get; set; }

        // Add a property to store the binary JPEG photo
        public byte[]? Photo { get; set; }
    }
}
