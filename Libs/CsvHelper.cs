namespace CSVWorker.Libs
{
    public static class CsvHelper
    {
        /// <summary>
        /// Checks if the provided file is a valid CSV file based on its extension and whether it is a file at all.
        /// </summary>
        /// <param name="file">The file to check.</param>
        /// <returns>True if the file is a valid CSV file; otherwise, false.</returns>
        public static bool IsValidCSV(IFormFile? file)
        {
            if (!IsFile(file))
                return false;

            return string.Equals(Path.GetExtension(file!.FileName), ".csv", StringComparison.OrdinalIgnoreCase);
        }

        /// <summary>
        /// Checks if the provided file is not null and has a non-negative length, indicating that it is a valid file.
        /// </summary>
        /// <param name="file">The file to check.</param>
        /// <returns>True if the file is a valid file; otherwise, false.</returns>
        private static bool IsFile(IFormFile? file)
        {
            return file is not null && file.Length > 0;
        }

        /// <summary>
        /// Parses a single line of CSV data into an array of strings based on the specified delimiter. If the input line is null or whitespace, it returns null.
        /// </summary>
        /// <param name="row">The line of CSV data to parse.</param>
        /// <param name="delimiter">The delimiter to use for splitting the line.</param>
        /// <returns>An array of strings representing the values in the CSV row, or null if the row is empty or whitespace.</returns>
        public static string[]? ParseLine(string row, char delimiter = ',')
        {
            if (string.IsNullOrWhiteSpace(row)) return null;

            return row.Split(delimiter);
        }

        /// <summary>
        /// Converts a list of string arrays (representing rows of CSV data) into a byte array containing the CSV content, using the specified delimiter to separate values. Each row is written as a line in the resulting CSV output.
        /// </summary>
        /// <param name="data">The list of string arrays representing the CSV rows.</param>
        /// <param name="delimiter">The delimiter to use for separating values.</param>
        /// <returns>A byte array containing the CSV content.</returns>
        public static async Task<byte[]> ConvertListToCsv(IEnumerable<string[]> data, char delimiter = ',', CancellationToken cancellationToken = default)
        {
            using var memoryStream = new MemoryStream();
            using var streamWriter = new StreamWriter(memoryStream);
            foreach (var row in data)
            {
                // Throw if cancellation is requested to allow cooperative cancellation of the operation.
                cancellationToken.ThrowIfCancellationRequested();

                var line = string.Join(delimiter, row);
                await streamWriter.WriteLineAsync(line);
            }
            await streamWriter.FlushAsync(cancellationToken);
            return memoryStream.ToArray();
        }
    }
}