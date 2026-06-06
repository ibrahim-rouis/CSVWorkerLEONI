namespace CSVWorker.Models.DTO
{
    public record ForsMaterial
    {
        public required string ProductNumber { get; init; }
        public required string PartNumber { get; init; }

        public required string MaterialClass { get; init; }

        public int Quantity { get; init; }

        public required string MU { get; init; }

        public double Weight { get; init; }

        public string? NodeID { get; set; }
    }
}
