using CSVWorker.Configuration;
using CSVWorker.Exceptions;
using CSVWorker.Libs;
using CSVWorker.Models;
using CSVWorker.Models.DTO;
using CSVWorker.Models.Entities;
using CSVWorker.Models.ViewModels.IMDSMacros;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using System.Text.RegularExpressions;

namespace CSVWorker.Services
{
    public partial class IMDSDatabaseService
    {
        private readonly ILogger<IMDSDatabaseService> _logger;
        private readonly CSVWorkerDBContext _context;
        private readonly CSVWorkerConfig _config;

        // Regular expression of a primary part number
        [GeneratedRegex(@"^P(\d+)[A-Za-z]*$")]
        private static partial Regex PartNumberRegex();

        public IMDSDatabaseService(ILogger<IMDSDatabaseService> logger, CSVWorkerDBContext context, IOptions<CSVWorkerConfig> config)
        {
            _logger = logger;
            _context = context;
            _config = config.Value;
        }

        /// <summary>
        /// Updates the database by processing and merging data from LPCP and A2 CSV files
        /// </summary>
        /// <param name="model">The view model containing the LPCP and A2 files to process.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <exception cref="CSVWorkerArgumentException">Thrown when either the LPCP file or A2 file is null.</exception>
        /// <exception cref="CSVWorkerInvalidDataException">Thrown when the header of the LPCP or A2 file is invalid or empty.</exception>
        public async Task UpdateDatabaseIMDS(UpdateDatabaseVM model, string? username, CancellationToken cancellationToken)
        {
            _logger.LogDebug("UpdateDatabase started. LPCP file={LPCPFileName}, A2 file={A2FileName}", model.LPCPFile?.FileName, model.A2File?.FileName);

            if (model.LPCPFile == null || model.A2File == null)
            {
                throw new CSVWorkerArgumentException("Input must not be null. Usually it must be validated in controller before sending to service for processing.");
            }

            var lpcpRecords = new List<LPCPRecord>();
            var a2Records = new List<A2Record>();

            // LPCP headers indexes.
            // we will determine the actual indexes dynamically at runtime by reading the header row.
            int lpcpLeoniPartIndex;
            int lpcpForsPnIndex;
            int lpcpSigipPnIndex;
            int lpcpVisualPnIndex;
            int lpcpWGKIndex;

            // A2 headers indexes
            // we will determine the actual indexes dynamically at runtime by reading the header row.
            int a2PartNumberIndex;
            int a2NodeIdIndex;

            /** Load LPCP and A2 CSV files **/

            /** Load LPCP **/
            using (var stream = model.LPCPFile.OpenReadStream())
            {
                using (var readerRaw = new StreamReader(stream))
                {
                    // Read the entire LPCP CSV content as a string.
                    var csvString = await readerRaw.ReadToEndAsync();

                    // Normalize the CSV string to ensure no row is split into multiple lines
                    // due to embedded line breaks within quoted fields.
                    var normalizedCsvString = CsvHelper.NormalizeCsvString(csvString);

                    // Use StringReader to read the normalized CSV string line by line.
                    using var normalizedReader = new StringReader(normalizedCsvString);

                    /** Read header **/

                    // Get header and determine column indexes for LEONI part number, FORS PN, SIGIP PN and Visual PN.
                    var headerLine = await normalizedReader.ReadLineAsync();

                    if (string.IsNullOrEmpty(headerLine))
                    {
                        throw new CSVWorkerInvalidDataException("LPCP file header is invalid or empty.");
                    }

                    // Detect delimiter (it is usually ';' but we want to be sure, and handle cases where it can be ',').
                    var delimiter = CsvHelper.DetectDelimiter(headerLine);

                    // Split header line into columns using the detected delimiter. 
                    var headerRow = CsvHelper.ParseLine(headerLine, delimiter);
                    if (headerRow == null || headerRow.Length == 0)
                    {
                        throw new CSVWorkerInvalidDataException("LPCP file header is invalid or empty.");
                    }

                    // Get required column indexes by header names.
                    lpcpLeoniPartIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "LEONI Part Number", "PART/ITEM NO/", "PART/ITEM NO/.", "Item- /Mat.-No." });
                    lpcpForsPnIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "FORS Part Number" });
                    lpcpSigipPnIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "SIGIP Part Number" });
                    lpcpVisualPnIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Visual Part Number" });
                    lpcpWGKIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "WGK" });

                    /** End Read header **/

                    // Read data line by line asynchronously
                    string? line;
                    while ((line = await normalizedReader.ReadLineAsync()) != null)
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        // Parse each line into columns using the detected delimiter and add to lpcpDoc list
                        var row = CsvHelper.ParseLine(line, delimiter);
                        if (row != null)
                        {
                            lpcpRecords.Add(new LPCPRecord
                            {
                                PartNumber = row[lpcpLeoniPartIndex].Trim(),
                                ForsPN = row[lpcpForsPnIndex].Trim(),
                                SIGIPPN = row[lpcpSigipPnIndex].Trim(),
                                VisualPN = row[lpcpVisualPnIndex].Trim(),
                                WGK = row[lpcpWGKIndex].Trim()
                            });
                        }

                    }
                }
            }

            /** END Load LPCP **/

            /** Load A2 **/
            using (var stream = model.A2File.OpenReadStream())
            {
                using (var reader = new StreamReader(stream))
                {
                    // Read the entire LPCP CSV content as a string.
                    var csvString = await reader.ReadToEndAsync();

                    // Normalize the CSV string to ensure no row is split into multiple lines
                    // due to embedded line breaks within quoted fields.
                    var normalizedCsvString = CsvHelper.NormalizeCsvString(csvString);

                    // Use StringReader to read the normalized CSV string line by line.
                    using var normalizedReader = new StringReader(normalizedCsvString);

                    /** Read header **/

                    // Get header
                    var headerLine = await normalizedReader.ReadLineAsync();
                    if (string.IsNullOrEmpty(headerLine))
                    {
                        throw new CSVWorkerInvalidDataException("A2 file header is invalid or empty.");
                    }

                    // Detect delimiter (it is usually ';' but we want to be sure, and handle cases where it can be ',').
                    var delimiter = CsvHelper.DetectDelimiter(headerLine);

                    // Split header line into columns using the detected delimiter.
                    var headerRow = CsvHelper.ParseLine(headerLine, delimiter);
                    if (headerRow == null || headerRow.Length == 0)
                    {
                        throw new CSVWorkerInvalidDataException("A2 file header is invalid or empty.");
                    }

                    // Get required column indexes by header names.
                    a2PartNumberIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "LP", "PART/ITEM NO/", "PART/ITEM NO/.", "LEONI Part Number" });
                    a2NodeIdIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Noeud", "Nœud", "Node ID" });

                    // Read the file line by line asynchronously
                    string? line;
                    while ((line = await normalizedReader.ReadLineAsync()) != null)
                    {
                        cancellationToken.ThrowIfCancellationRequested();

                        // Split line into columns using the detected delimiter
                        var row = CsvHelper.ParseLine(line, delimiter);

                        if (row != null)
                        {
                            var a2LPValue = CsvHelper.TryGetValue(row, a2PartNumberIndex);

                            /** Handle special cases where line is like: "PXXX;PXXX";NODEIDXXX **/

                            // Sometimes the A2 file contains lines where the LP column
                            // has multiple part numbers separated by ';' within the same cell,
                            // like: "PXXX;PXXX";NODEIDXXX
                            if (a2LPValue != null && a2LPValue.Contains(";"))
                            {
                                var multiparts = a2LPValue.Trim('\"').Trim().Split(';', StringSplitOptions.RemoveEmptyEntries);
                                if (multiparts.Length > 0)
                                {
                                    foreach (var part in multiparts)
                                    {
                                        var trimmedPart = part.Trim();
                                        if (string.IsNullOrWhiteSpace(trimmedPart))
                                        {
                                            continue;
                                        }

                                        // Keep row shape compatible with dynamic indexes used later.
                                        var requiredLength = Math.Max(row.Length, Math.Max(a2PartNumberIndex, a2NodeIdIndex) + 1);
                                        var normalizedRow = new string[requiredLength];

                                        // Optional: preserve other original columns.
                                        Array.Copy(row, normalizedRow, row.Length);

                                        normalizedRow[a2PartNumberIndex] = trimmedPart;
                                        normalizedRow[a2NodeIdIndex] = CsvHelper.TryGetValue(row, a2NodeIdIndex) ?? string.Empty;

                                        a2Records.Add(new A2Record
                                        {
                                            PartNumber = normalizedRow[a2PartNumberIndex].Trim(),
                                            NodeID = normalizedRow[a2NodeIdIndex].Trim()
                                        });
                                    }
                                }
                            }
                            /** END Handle special cases where line is like: "PXXX;PXXX";NODEIDXXX **/
                            else
                            {
                                // Normal case, just add the row as is.
                                a2Records.Add(new A2Record
                                {
                                    PartNumber = row[a2PartNumberIndex].Trim(),
                                    NodeID = row[a2NodeIdIndex].Trim()
                                });
                            }
                        }
                    }
                }
            }

            /** END Load A2 **/

            /** End Load LPCP and A2 CSV files **/

            // Cleanup duplicates from LPCP records based on all part number columns
            lpcpRecords = [.. lpcpRecords
                .GroupBy(r => new
                {
                    PartNumber = r.PartNumber?.Trim(),
                    ForsPN = r.ForsPN?.Trim(),
                    SIGIPPN = r.SIGIPPN?.Trim(),
                    VisualPN = r.VisualPN?.Trim(),
                    WGK = r.WGK?.Trim()
                })
                .Select(g => g.First())];

            _logger.LogDebug($"There are {lpcpRecords.Count} unique rows in LPCP.");

            // Optimize A2 Lookup: Build a dictionary for O(1) lookups
            var a2Lookup = a2Records
                .Where(a => !string.IsNullOrEmpty(a.PartNumber))
                .GroupBy(a => a.PartNumber!)
                .ToDictionary(g => g.Key, g => g.First().NodeID);

            var recordsToAdd = new List<IMDSDatabaseRecord>();
            var recordsToUpdate = new List<IMDSDatabaseRecord>();

            // Load whole IMDS database into memory for faster lookups during processing.
            var _existingIMDSRecords = await _context.IMDSDatabase.ToListAsync(cancellationToken);

            // helper
            string Normalize(string? s) => string.IsNullOrWhiteSpace(s) ? string.Empty : s.Trim();
            string BuildKey(string? pn, string? fors, string? sigip, string? visual, string? wgk)
                => $"{Normalize(pn)}|{Normalize(fors)}|{Normalize(sigip)}|{Normalize(visual)}|{Normalize(wgk)}";

            var existingLookup = _existingIMDSRecords.ToDictionary(
                e => BuildKey(e.PartNumber, e.ForsPN, e.SIGIPPN, e.VisualPN, e.WGK),
                e => e);

            foreach (var lpcpRow in lpcpRecords)
            {
                cancellationToken.ThrowIfCancellationRequested();

                // Skip if all properties in record are null or empty
                if (string.IsNullOrEmpty(lpcpRow.PartNumber)
                    && string.IsNullOrEmpty(lpcpRow.ForsPN)
                    && string.IsNullOrEmpty(lpcpRow.VisualPN)
                    && string.IsNullOrEmpty(lpcpRow.SIGIPPN)
                    && string.IsNullOrEmpty(lpcpRow.WGK))
                {
                    continue;
                }

                // Fast Node ID Lookup
                string? nodeID = null;
                if (lpcpRow.PartNumber != null && a2Lookup.TryGetValue(lpcpRow.PartNumber, out var val1)) nodeID = val1;
                else if (lpcpRow.ForsPN != null && a2Lookup.TryGetValue(lpcpRow.ForsPN, out var val2)) nodeID = val2;
                else if (lpcpRow.SIGIPPN != null && a2Lookup.TryGetValue(lpcpRow.SIGIPPN, out var val3)) nodeID = val3;
                else if (lpcpRow.VisualPN != null && a2Lookup.TryGetValue(lpcpRow.VisualPN, out var val4)) nodeID = val4;
                else if (lpcpRow.WGK != null && a2Lookup.TryGetValue(lpcpRow.WGK, out var val5)) nodeID = val5;

                var key = BuildKey(lpcpRow.PartNumber, lpcpRow.ForsPN, lpcpRow.SIGIPPN, lpcpRow.VisualPN, lpcpRow.WGK);

                if (existingLookup.TryGetValue(key, out var exisitingRecord))
                {
                    // Only update if nodeID changed
                    if (exisitingRecord.NodeID != nodeID)
                    {
                        exisitingRecord.NodeID = nodeID;
                        exisitingRecord.LastUpdatedBy = username;
                        exisitingRecord.LastUpdatedAt = DateTime.UtcNow;

                        recordsToUpdate.Add(exisitingRecord);
                    }
                }
                else
                {
                    // Add new record
                    var newRecord = new IMDSDatabaseRecord
                    {
                        PartNumber = lpcpRow.PartNumber,
                        ForsPN = lpcpRow.ForsPN,
                        SIGIPPN = lpcpRow.SIGIPPN,
                        VisualPN = lpcpRow.VisualPN,
                        WGK = lpcpRow.WGK,
                        NodeID = nodeID,
                        CreatedAt = DateTime.UtcNow,
                        LastUpdatedAt = DateTime.UtcNow,
                        createdBy = username,
                        LastUpdatedBy = username
                    };

                    recordsToAdd.Add(newRecord);
                }
            }

            // Batch Save
            _context.ChangeTracker.AutoDetectChangesEnabled = false; // Speeds up large inserts
            try
            {
                if (recordsToAdd.Any())
                {
                    await _context.IMDSDatabase.AddRangeAsync(recordsToAdd);
                }

                if (recordsToUpdate.Any())
                {
                    _context.IMDSDatabase.UpdateRange(recordsToUpdate);
                }

                await _context.SaveChangesAsync();
            }
            finally
            {
                _context.ChangeTracker.AutoDetectChangesEnabled = true;
            }

            _logger.LogDebug("Update IMDS Database finished.");
        }

        // Get by ID
        public async Task<IMDSDatabaseRecord?> GetByIdAsync(long id)
        {
            return await _context.IMDSDatabase.FirstOrDefaultAsync(p => p.Id == id);
        }

        /// <summary>
        /// Removes trailing letters from the end of a string if it starts with 'P' 
        /// followed by at least one number, and ends with optional letters.
        /// </summary>
        /// <param name="input">The input string.</param>
        /// <returns>The string with trailing letters removed if it matches the pattern; otherwise, the original string.</returns>
        private string RemoveTrailingLetters(string input)
        {
            if (string.IsNullOrEmpty(input))
                return input;

            // Use the generated regex to match the pattern
            var match = PartNumberRegex().Match(input);

            if (match.Success)
            {
                // Returns 'P' + the numbers captured in Group 1
                return "P" + match.Groups[1].Value;
            }

            return input;
        }

        // Find NodeID
        public async Task<string?> FindNodeIDbyAny(string query)
        {
            var found = await _context.IMDSDatabase
                .OrderByDescending(p => p.LastUpdatedAt)
                .Where(p =>
                (
                    p.PartNumber == query ||
                    p.ForsPN == query ||
                    p.SIGIPPN == query ||
                    p.VisualPN == query ||
                    p.WGK == query
                ))
                .FirstOrDefaultAsync();

            if (found != null && !string.IsNullOrEmpty(found.NodeID))
            {
                return found.NodeID;
            }

            string trimmedPartNumber = RemoveTrailingLetters(query);
            if (query != trimmedPartNumber)
            {
                return await FindNodeIDbyAny(trimmedPartNumber);
            }
            else
            {
                return null;
            }
        }

        public async Task<IMDSDatabaseRecord?> GetByPartNumbersAsync(string? partNumber, string? forstPN, string? sigipPN, string? visualPN, string? wgk)
        {
            var record = await _context.IMDSDatabase.FirstOrDefaultAsync(r =>
                r.PartNumber == partNumber
                && r.ForsPN == forstPN
                && r.SIGIPPN == sigipPN
                && r.VisualPN == visualPN
                && r.WGK == wgk);
            if (record == null)
            {
                return null;
            }
            return record;
        }

        public async Task<IMDSDatabaseRecord> SaveAsync(IMDSDatabaseRecord record, string? username)
        {
            // Check if already exists
            if (await Exists(record))
            {
                throw new CSVWorkerArgumentException("Record with the same part numbers already exists.");
            }

            record.CreatedAt = DateTime.UtcNow;
            record.createdBy = username;
            record.LastUpdatedAt = DateTime.UtcNow;
            record.LastUpdatedBy = username;

            var entry = await _context.IMDSDatabase.AddAsync(record);
            await _context.SaveChangesAsync();
            return entry.Entity;
        }

        public async Task<bool> Exists(IMDSDatabaseRecord record)
        {
            return await _context.IMDSDatabase.AnyAsync(p => (p.PartNumber == record.PartNumber
                && p.ForsPN == record.ForsPN
                && p.SIGIPPN == record.SIGIPPN
                && p.VisualPN == record.VisualPN
                && p.WGK == record.WGK));
        }

        public async Task DeleteAsync(long id)
        {
            var record = await _context.IMDSDatabase.FindAsync(id);
            if (record == null)
            {
                throw new CSVWorkerArgumentException($"Record with ID {id} not found for deletion.");
            }
            _context.IMDSDatabase.Remove(record);
            await _context.SaveChangesAsync();
        }

        public async Task<IMDSDatabaseRecord> UpdateAsync(IMDSDatabaseRecord record, string? username)
        {
            var existingRecord = await _context.IMDSDatabase.FindAsync(record.Id);

            if (existingRecord == null)
            {
                throw new CSVWorkerArgumentException($"Record with ID {record.Id} not found for update.");
            }

            // Update properties
            existingRecord.PartNumber = record.PartNumber;
            existingRecord.ForsPN = record.ForsPN;
            existingRecord.SIGIPPN = record.SIGIPPN;
            existingRecord.VisualPN = record.VisualPN;
            existingRecord.WGK = record.WGK;
            existingRecord.NodeID = record.NodeID;
            existingRecord.LastUpdatedAt = DateTime.UtcNow;
            existingRecord.LastUpdatedBy = username;

            _context.IMDSDatabase.Update(existingRecord);
            await _context.SaveChangesAsync();

            return existingRecord;
        }

        // Offset-pagination with cancellation and basic validation.
        public async Task<PagedResult<IMDSDatabaseRecord>> GetPagedAsync(int? pageNumber, string? query)
        {
            var pageSize = _config.DefaultPageSize;

            if (pageNumber == null || pageNumber < 1) pageNumber = 1;

            IQueryable<IMDSDatabaseRecord> q = _context.IMDSDatabase;

            if (!string.IsNullOrWhiteSpace(query))
            {
                q = q.Where(r =>
                    (r.PartNumber != null && r.PartNumber.Contains(query)) ||
                    (r.ForsPN != null && r.ForsPN.Contains(query)) ||
                    (r.SIGIPPN != null && r.SIGIPPN.Contains(query)) ||
                    (r.VisualPN != null && r.VisualPN.Contains(query)) ||
                    (r.WGK != null && r.WGK.Contains(query)) ||
                    (r.NodeID != null && r.NodeID.Contains(query)));
            }

            var total = await q.CountAsync();
            var items = await q
                .OrderByDescending(r => r.LastUpdatedAt) // apply ordering after filtering
                .Skip((pageNumber.Value - 1) * pageSize)
                .Take(pageSize)
                .ToListAsync();

            return new PagedResult<IMDSDatabaseRecord>(items, total, pageNumber.Value, pageSize);
        }

        // Export Database
        public async Task<byte[]> ExportDatabaseAsync(CancellationToken cancellationToken)
        {
            var allRecords = await _context.IMDSDatabase.ToListAsync(cancellationToken);

            var exportedDatabase = new List<string[]>
            {
                (new string[] { "Node ID", "x", "PART/ITEM NO/", "FORS PN", "SIGIP PN", "Visual PN", "WGK", "last Status Date", "Weight", "Weight Unit" })
            };

            foreach (var record in allRecords)
            {
                exportedDatabase.Add(new string[]
                {
                    record.NodeID ?? string.Empty,
                    string.Empty,
                    record.PartNumber ?? string.Empty,
                    record.ForsPN ?? string.Empty,
                    record.SIGIPPN ?? string.Empty,
                    record.VisualPN ?? string.Empty,
                    record.WGK ?? string.Empty,
                    string.Empty,
                    string.Empty,
                    string.Empty
                });
            }

            var outputBytes = await CsvHelper.ConvertListToCsv(exportedDatabase, ';');

            return outputBytes;
        }
    }
}
