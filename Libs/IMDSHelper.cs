using System.Text.RegularExpressions;

namespace CSVWorker.Libs
{
    public interface IMDSHelper
    {
        static string? GetNodeID(string partNumber, Dictionary<string, IReadOnlyList<string>> databaseByLeoniPart, Dictionary<string, IReadOnlyList<string>> databaseByFORSPN, Dictionary<string, IReadOnlyList<string>> databaseBySIGIPN, Dictionary<string, IReadOnlyList<string>> databaseByVisualPN, Dictionary<string, IReadOnlyList<string>> databaseByWGK, int databaseNodeIdIndex)
        {
            // Remove trailing letters from part number, for example P0123456AA should become P0123456
            partNumber = RemoveTrailingLetters(partNumber);

            // Find Node ID for this part number from the Database CSV, if available
            string nodeId = string.Empty;
            if (databaseByLeoniPart.TryGetValue(partNumber, out var databaseRow))
            {
                nodeId = CsvHelper.TryGetValue([.. databaseRow], databaseNodeIdIndex) ?? string.Empty;
            }
            if (string.IsNullOrWhiteSpace(nodeId) && databaseByFORSPN.TryGetValue(partNumber, out var databaseRowFORS))
            {
                nodeId = CsvHelper.TryGetValue([.. databaseRowFORS], databaseNodeIdIndex) ?? string.Empty;
            }
            if (string.IsNullOrWhiteSpace(nodeId) && databaseBySIGIPN.TryGetValue(partNumber, out var databaseRowSIGIP))
            {
                nodeId = CsvHelper.TryGetValue([.. databaseRowSIGIP], databaseNodeIdIndex) ?? string.Empty;
            }
            if (string.IsNullOrWhiteSpace(nodeId) && databaseByVisualPN.TryGetValue(partNumber, out var databaseRowVisual))
            {
                nodeId = CsvHelper.TryGetValue([.. databaseRowVisual], databaseNodeIdIndex) ?? string.Empty;
            }
            if (string.IsNullOrWhiteSpace(nodeId) && databaseByWGK.TryGetValue(partNumber, out var databaseRowWGK))
            {
                nodeId = CsvHelper.TryGetValue([.. databaseRowWGK], databaseNodeIdIndex) ?? string.Empty;
            }

            // if nodeId is empty or white space we change it to "#N/A" and add it to missing nodes 
            if (string.IsNullOrWhiteSpace(nodeId))
            {
                return null;
            }

            return nodeId;
        }

        /// <summary>
        /// Removes trailing letters from the end of a string if it starts with 'P' 
        /// followed by at least one number, and ends with optional letters.
        /// </summary>
        /// <param name="input">The input string.</param>
        /// <returns>The string with trailing letters removed if it matches the pattern; otherwise, the original string.</returns>
        static string RemoveTrailingLetters(string input)
        {
            if (string.IsNullOrEmpty(input))
                return input;

            // Pattern breakdown:
            // ^P         - Starts with 'P'
            // (\d+)      - Followed by at least one number (captured in Group 1)
            // [A-Za-z]*$ - Followed by optional letters until the end of the string
            var match = Regex.Match(input, @"^P(\d+)[A-Za-z]*$");

            if (match.Success)
            {
                // Returns 'P' + the numbers captured in Group 1
                return "P" + match.Groups[1].Value;
            }

            return input;
        }

        /// <summary>
        /// Resizes the row to the specified size and fills new cells with empty strings if the original row is shorter than the specified size.
        /// </summary>
        /// <param name="row">The row to resize.</param>
        /// <param name="size">The desired size of the row.</param>
        static void ResizeAndFillRows(ref string[] row, int size)
        {
            var originalLength = row.Length;

            Array.Resize(ref row, size);

            if (originalLength < size)
            {
                Array.Fill(row, string.Empty, originalLength, size - originalLength);
            }
        }
    }
}
