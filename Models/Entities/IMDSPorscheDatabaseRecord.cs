using System.ComponentModel.DataAnnotations;

namespace CSVWorker.Models.Entities
{
    public class IMDSPorscheDatabaseRecord
    {
        public long Id { get; set; }

        [StringLength(255)]
        public string? PartNumber { get; set; }

        [StringLength(255)]
        public string? ArticleName { get; set; }

        [StringLength(255)]
        public string? MaterialGroup { get; set; }

        [StringLength(255)]
        public string? CrossSec { get; set; }

        public DateTime CreatedAt { get; set; }
        public DateTime? LastUpdatedAt { get; set; }
    }
}
