namespace CSVWorker.Models.DTO
{
    public record LPCPRecord
    {
        public string? PartNumber { get; init; }
        public string? ForsPN { get; init; }

        public string? SIGIPPN { get; init; }

        public string? VisualPN { get; init; }

        public string? WGK { get; init; }
    }
}
