namespace CSVWorker.Services.CSVHelper
{
    public interface ICsvHelperService
    {
        Task<CsvDocument> ReadAsync(Stream stream, bool hasHeader = true, char delimiter = ',', CancellationToken cancellationToken = default);

        IAsyncEnumerable<IReadOnlyList<string>> ReadRowsAsync(Stream stream, char delimiter = ',', CancellationToken cancellationToken = default);

        Task WriteAsync(Stream stream, IEnumerable<IEnumerable<string?>> rows, char delimiter = ',', CancellationToken cancellationToken = default);

        string SerializeRow(IEnumerable<string?> fields, char delimiter = ',');

        IReadOnlyList<string> ParseRow(string row, char delimiter = ',');

        int FindHeaderIndex(IReadOnlyList<string> headers, params string[] acceptedNames);

        string NormalizeHeader(string? value);

        string GetValue(IReadOnlyList<string> row, int index);

    }

    public sealed class CsvDocument
    {
        public CsvDocument(IReadOnlyList<string> headers, IReadOnlyList<IReadOnlyList<string>> rows)
        {
            Headers = headers;
            Rows = rows;
        }

        public IReadOnlyList<string> Headers { get; }

        public IReadOnlyList<IReadOnlyList<string>> Rows { get; }
    }
}
