using System.ComponentModel.DataAnnotations;

namespace CSVWorker.Models.Entities
{
    public class IMDSDatabaseRecord
    {
        public long Id { get; set; }

        [StringLength(255)]
        public string? PartNumber { get; set; }

        [StringLength(255)]
        public string? ForsPN { get; set; }

        [StringLength(255)]
        public string? SIGIPPN { get; set; }

        [StringLength(255)]
        public string? VisualPN { get; set; }

        [StringLength(255)]
        public string? WGK { get; set; }

        [StringLength(255)]
        public string? NodeID { get; set; }

        public DateTime CreatedAt { get; set; } = DateTime.Now;
        public DateTime LastUpdatedAt { get; set; }

        [StringLength(255)]
        public string? createdBy { get; set; }

        [StringLength(255)]
        public string? LastUpdatedBy { get; set; }
    }
}
