using Microsoft.EntityFrameworkCore;
using System.ComponentModel.DataAnnotations;

namespace CSVWorker.Models.Entities
{
    // Set a unique index on the Name property
    [Index(nameof(Name), IsUnique = true)]
    public class Role
    {
        public int Id { get; set; }

        [Required]
        [StringLength(255)] // Good practice for indexable string columns
        public string? Name { get; set; }

        public List<User>? Users { get; set; }
    }
}
