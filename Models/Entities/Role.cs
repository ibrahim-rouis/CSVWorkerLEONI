using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;

namespace CSVWorker.Models.Entities
{
    // Set a unique index on the Name property
    [Index(nameof(Name), IsUnique = true)]
    public class Role
    {
        public long Id { get; set; }

        [Required]
        [StringLength(255)]
        [MinLength(1)]
        public string Name { get; set; } = string.Empty;

        public ICollection<User> Users { get; set; } = [];

        // CreatedAt timestamp for the role record
        public DateTime CreatedAt { get; set; }
    }
}
