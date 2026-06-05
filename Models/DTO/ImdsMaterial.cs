namespace CSVWorker.Models.DTO
{
    public record ImdsMaterial
    {
        public required string PartNumber { get; init; }
        public string ArticleName { get; set; } = "#N/A";
        public required string ComponentType { get; init; }
        public string? CrossSec { get; set; }

        public double Weight { get; init; } = 0.0;
        public int Quantity { get; init; } = 0;

        public string NodeID { get; set; } = "#N/A";
    }
}
