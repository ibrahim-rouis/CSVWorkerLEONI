namespace CSVWorker.Models.DTO
{
    public record PorscheCSVRecord
    {
        public string? PartNumber { get; init; }
        public string? ArticleName { get; init; }

        public string? FORSMaterialClass { get; init; }

        public string? CrossSec { get; init; }
    }
}
