namespace CSVWorker.Libs
{
    public interface IMDSHelper
    {
        static string? GetNodeID(string partNumber, Dictionary<string, IReadOnlyList<string>> databaseByLeoniPart, Dictionary<string, IReadOnlyList<string>> databaseByFORSPN, Dictionary<string, IReadOnlyList<string>> databaseBySIGIPN, Dictionary<string, IReadOnlyList<string>> databaseByVisualPN, Dictionary<string, IReadOnlyList<string>> databaseByWGK, int databaseNodeIdIndex)
        {
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
    }
}
