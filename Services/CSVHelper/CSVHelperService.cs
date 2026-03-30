using System.Globalization;
using System.Runtime.CompilerServices;
using System.Text;

namespace CSVWorker.Services.CSVHelper
{
    public class CsvHelperService : ICsvHelperService
    {
        private readonly ILogger<CsvHelperService> _logger;

        public CsvHelperService(ILogger<CsvHelperService> logger)
        {
            _logger = logger;
        }

        public async Task<CsvDocument> ReadAsync(Stream stream, bool hasHeader = true, char delimiter = ',', CancellationToken cancellationToken = default)
        {
            using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);

            var headers = new List<string>();
            var rows = new List<IReadOnlyList<string>>();

            var firstRecord = await ReadRecordAsync(reader, delimiter, cancellationToken);
            if (firstRecord == null)
            {
                return new CsvDocument(headers, rows);
            }

            if (hasHeader)
            {
                headers.AddRange(firstRecord);
            }
            else
            {
                rows.Add(firstRecord);
            }

            while (true)
            {
                var record = await ReadRecordAsync(reader, delimiter, cancellationToken);
                if (record == null)
                {
                    break;
                }

                rows.Add(record);
            }

            _logger.LogInformation("CSV parsed. Headers={HeaderCount}, Rows={RowCount}", headers.Count, rows.Count);
            return new CsvDocument(headers, rows);
        }

        public async IAsyncEnumerable<IReadOnlyList<string>> ReadRowsAsync(
            Stream stream,
            char delimiter = ',',
            [EnumeratorCancellation] CancellationToken cancellationToken = default)
        {
            using var reader = new StreamReader(stream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, leaveOpen: true);

            while (true)
            {
                var record = await ReadRecordAsync(reader, delimiter, cancellationToken);
                if (record == null)
                {
                    yield break;
                }

                yield return record;
            }
        }

        public async Task WriteAsync(Stream stream, IEnumerable<IEnumerable<string?>> rows, char delimiter = ',', CancellationToken cancellationToken = default)
        {
            using var writer = new StreamWriter(stream, Encoding.UTF8, leaveOpen: true);

            foreach (var row in rows)
            {
                cancellationToken.ThrowIfCancellationRequested();
                await writer.WriteLineAsync(SerializeRow(row, delimiter));
            }

            await writer.FlushAsync(cancellationToken);
        }

        public string SerializeRow(IEnumerable<string?> fields, char delimiter = ',')
        {
            return string.Join(delimiter, fields.Select(field =>
            {
                var value = field ?? string.Empty;
                var mustQuote = value.Contains(delimiter) || value.Contains('"') || value.Contains('\r') || value.Contains('\n');

                if (!mustQuote)
                {
                    return value;
                }

                return $"\"{value.Replace("\"", "\"\"")}\"";
            }));
        }

        public IReadOnlyList<string> ParseRow(string row, char delimiter = ',')
        {
            var result = new List<string>();
            var current = new StringBuilder();
            var inQuotes = false;

            for (var i = 0; i < row.Length; i++)
            {
                var c = row[i];

                if (c == '"')
                {
                    if (inQuotes && i + 1 < row.Length && row[i + 1] == '"')
                    {
                        current.Append('"');
                        i++;
                        continue;
                    }

                    inQuotes = !inQuotes;
                    continue;
                }

                if (c == delimiter && !inQuotes)
                {
                    result.Add(current.ToString());
                    current.Clear();
                    continue;
                }

                current.Append(c);
            }

            if (inQuotes)
            {
                throw new FormatException("Invalid CSV row: unmatched quote.");
            }

            result.Add(current.ToString());
            return result;
        }

        private static async Task<IReadOnlyList<string>?> ReadRecordAsync(TextReader reader, char delimiter, CancellationToken cancellationToken)
        {
            var fields = new List<string>();
            var current = new StringBuilder();
            var inQuotes = false;
            var hasData = false;
            var buffer = new char[1];

            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();

                int cInt = reader.Read();

                if (cInt == -1) // End of stream
                {
                    if (!hasData && current.Length == 0 && fields.Count == 0) return null;
                    if (inQuotes) throw new FormatException("Unmatched quote at EOF.");

                    fields.Add(current.ToString());
                    return fields;
                }

                hasData = true;
                char c = (char)cInt;

                if (c == '"')
                {
                    if (inQuotes && reader.Peek() == '"')
                    {
                        _ = reader.Read(); // consume escaped quote
                        current.Append('"');
                        continue;
                    }

                    inQuotes = !inQuotes;
                    continue;
                }

                if (c == delimiter && !inQuotes)
                {
                    fields.Add(current.ToString());
                    current.Clear();
                    continue;
                }

                if ((c == '\n' || c == '\r') && !inQuotes)
                {
                    if (c == '\r' && reader.Peek() == '\n')
                    {
                        _ = reader.Read();
                    }

                    fields.Add(current.ToString());
                    return fields;
                }

                current.Append(c);
            }
        }

        public string GetValue(IReadOnlyList<string> row, int index)
        {
            if (index < 0 || index >= row.Count)
            {
                return string.Empty;
            }

            return row[index]?.Trim() ?? string.Empty;
        }

        public int FindHeaderIndex(IReadOnlyList<string> headers, params string[] acceptedNames)
        {
            for (var i = 0; i < headers.Count; i++)
            {
                var current = NormalizeHeader(headers[i]);
                if (acceptedNames.Any(name => NormalizeHeader(name) == current))
                {
                    return i;
                }
            }

            throw new InvalidOperationException($"Required header not found. Expected one of: {string.Join(", ", acceptedNames)}");
        }

        public string NormalizeHeader(string? value)
        {
            if (string.IsNullOrWhiteSpace(value))
            {
                return string.Empty;
            }

            var text = value.Trim().Replace("œ", "oe", StringComparison.OrdinalIgnoreCase);
            text = text.Normalize(NormalizationForm.FormD);

            var sb = new StringBuilder(text.Length);
            foreach (var c in text)
            {
                var uc = CharUnicodeInfo.GetUnicodeCategory(c);
                if (uc != UnicodeCategory.NonSpacingMark)
                {
                    sb.Append(char.ToLowerInvariant(c));
                }
            }

            return sb.ToString().Normalize(NormalizationForm.FormC);
        }
    }
}

