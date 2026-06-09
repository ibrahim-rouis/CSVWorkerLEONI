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
    public partial class IMDSPorscheDatabaseService
    {
        private readonly ILogger<IMDSPorscheDatabaseService> _logger;
        private readonly CSVWorkerDBContext _context;
        private readonly CSVWorkerConfig _config;

        // Regular expression of a primary part number
        [GeneratedRegex(@"^P(\d+)[A-Za-z]*$")]
        private static partial Regex PartNumberRegex();

        public IMDSPorscheDatabaseService(ILogger<IMDSPorscheDatabaseService> logger, CSVWorkerDBContext context, IOptions<CSVWorkerConfig> config)
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
        public async Task UpdatePorscheDatabase(UpdatePorscheDatabaseVM model, string? username, CancellationToken cancellationToken)
        {
            _logger.LogDebug("UpdatePorscheDatabase started. Porsche CSV file={PorscheCSVFileName}", model.PorscheCSV?.FileName);

            if (model.PorscheCSV == null)
            {
                throw new CSVWorkerArgumentException("Input must not be null. Usually it must be validated in controller before sending to service for processing.");
            }

            // Porsche CSV database indexes
            int databasePartNumberIndex;
            int databaseArticleNameIndex;
            int databaseForsMaterialClassIndex;
            int databaseCrossSecIndex;

            var porscheCSVDatabase = new List<PorscheCSVRecord>();

            using (var stream = model.PorscheCSV.OpenReadStream())
            {
                using (var readerRaw = new StreamReader(stream))
                {
                    // Read the entire CSV content as a string.
                    var csvString = await readerRaw.ReadToEndAsync(cancellationToken);

                    // Normalize the CSV string to ensure no row is split into multiple lines
                    // due to embedded line breaks within quoted fields.
                    var normalizedCsvString = CsvHelper.NormalizeCsvString(csvString);

                    // Use StringReader to read the normalized CSV string line by line.
                    using var reader = new StringReader(normalizedCsvString);

                    /** Read header **/
                    var headerLine = await reader.ReadLineAsync(cancellationToken);
                    if (string.IsNullOrEmpty(headerLine))
                    {
                        throw new CSVWorkerInvalidDataException("Database file header is invalid or empty.");
                    }

                    // Detect delimiter (it is usually ';' but we want to be sure, and handle cases where it can be ',').
                    var delimiter = CsvHelper.DetectDelimiter(headerLine);

                    // split header line into columns
                    var headerRow = CsvHelper.ParseLine(headerLine, delimiter);
                    if (headerRow == null || headerRow.Length == 0)
                    {
                        throw new CSVWorkerInvalidDataException("Database CSV file header is invalid or empty.");
                    }


                    // Get required column indexes by header names.
                    databasePartNumberIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Item- /Mat.-No.", "PART/ITEM NO/", "PART/ITEM NO/.", "LEONI Part Number" });
                    databaseArticleNameIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Article Name", "Description" });
                    databaseForsMaterialClassIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "FORS Material Group", "Material class" });
                    databaseCrossSecIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Cross-Sec (INDIV1)", "Cross-Sec" });


                    /** Finish reading header **/

                    // Read the file line by line asynchronously and add to database list
                    string? line;
                    while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                    {
                        var row = CsvHelper.ParseLine(line, delimiter);
                        if (row != null && !string.IsNullOrEmpty(row[databasePartNumberIndex]))
                        {
                            porscheCSVDatabase.Add(new PorscheCSVRecord
                            {
                                PartNumber = row[databasePartNumberIndex].Trim(),
                                ArticleName = row[databaseArticleNameIndex].Trim(),
                                FORSMaterialClass = row[databaseForsMaterialClassIndex].Trim(),
                                CrossSec = row[databaseCrossSecIndex].Trim(),
                            });
                        }
                    }
                }
            }

            /** Finish loading Porsche Database **/

            // Cleanup duplicates from Porsche CSV records based on Partnumber
            porscheCSVDatabase = [.. porscheCSVDatabase
                .GroupBy(r => new
                {
                    PartNumber = r.PartNumber?.Trim(),
                })
                .Select(g => g.First())];

            _logger.LogDebug($"There are {porscheCSVDatabase.Count} unique rows in Porsche CSV database.");

            var recordsToAdd = new List<IMDSPorscheDatabaseRecord>();
            var recordsToUpdate = new List<IMDSPorscheDatabaseRecord>();

            // Load whole Porsche IMDS database into memory for faster lookups during processing.
            var _existingIMDSRecords = await _context.IMDSPorscheDatabase.ToListAsync(cancellationToken);

            // helper
            string Normalize(string? s) => string.IsNullOrWhiteSpace(s) ? string.Empty : s.Trim();
            string BuildKey(string? pn)
                => $"{Normalize(pn)}";

            var existingLookup = _existingIMDSRecords.ToDictionary(
                e => BuildKey(e.PartNumber),
                e => e);

            foreach (var row in porscheCSVDatabase)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var key = BuildKey(row.PartNumber);

                if (existingLookup.TryGetValue(key, out var exisitingRecord))
                {
                    // Only update if Article Name or CrossSec changed
                    if (exisitingRecord.ArticleName != row.ArticleName || exisitingRecord.CrossSec != row.CrossSec)
                    {
                        exisitingRecord.ArticleName = row.ArticleName;
                        exisitingRecord.CrossSec = row.CrossSec;
                        exisitingRecord.LastUpdatedBy = username;
                        exisitingRecord.LastUpdatedAt = DateTime.UtcNow;

                        recordsToUpdate.Add(exisitingRecord);
                    }
                }
                else
                {
                    // Add new record
                    var newRecord = new IMDSPorscheDatabaseRecord
                    {
                        PartNumber = row.PartNumber,
                        ArticleName = row.ArticleName,
                        MaterialGroup = row.FORSMaterialClass,
                        CrossSec = row.CrossSec,
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
                    await _context.IMDSPorscheDatabase.AddRangeAsync(recordsToAdd);
                }

                if (recordsToUpdate.Any())
                {
                    _context.IMDSPorscheDatabase.UpdateRange(recordsToUpdate);
                }

                await _context.SaveChangesAsync();
            }
            finally
            {
                _context.ChangeTracker.AutoDetectChangesEnabled = true;
            }

            _logger.LogDebug("UpdatePorscheDatabase finished.");
        }

        // Get by ID
        public async Task<IMDSPorscheDatabaseRecord?> GetByIdAsync(long id)
        {
            return await _context.IMDSPorscheDatabase.FirstOrDefaultAsync(p => p.Id == id);
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

        // Find Article by Part Number
        public async Task<IMDSPorscheDatabaseRecord?> FindArticle(string partNumber)
        {
            var found = await _context.IMDSPorscheDatabase
                .OrderByDescending(p => p.LastUpdatedAt)
                .Where(p =>
                (
                    p.PartNumber == partNumber
                ))
                .FirstOrDefaultAsync();

            if (found != null)
            {
                return found;
            }

            string trimmedPartNumber = RemoveTrailingLetters(partNumber);
            if (partNumber != trimmedPartNumber)
            {
                return await FindArticle(trimmedPartNumber);
            }
            else
            {
                return null;
            }
        }

        public async Task<IMDSPorscheDatabaseRecord?> GetByPartNumberAsync(string partNumber)
        {
            var record = await _context.IMDSPorscheDatabase.FirstOrDefaultAsync(r => r.PartNumber == partNumber);
            if (record == null)
            {
                return null;
            }
            return record;
        }

        public async Task<IMDSPorscheDatabaseRecord> SaveAsync(IMDSPorscheDatabaseRecord record, string? username)
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

            var entry = await _context.IMDSPorscheDatabase.AddAsync(record);
            await _context.SaveChangesAsync();
            return entry.Entity;
        }

        public async Task<bool> Exists(IMDSPorscheDatabaseRecord record)
        {
            return await _context.IMDSPorscheDatabase.AnyAsync(p => p.PartNumber == record.PartNumber);
        }

        public async Task DeleteAsync(long id)
        {
            var record = await _context.IMDSPorscheDatabase.FindAsync(id);
            if (record == null)
            {
                throw new CSVWorkerArgumentException($"Record with ID {id} not found for deletion.");
            }
            _context.IMDSPorscheDatabase.Remove(record);
            await _context.SaveChangesAsync();
        }

        public async Task<IMDSPorscheDatabaseRecord> UpdateAsync(IMDSPorscheDatabaseRecord record, string? username)
        {
            var existingRecord = await _context.IMDSPorscheDatabase.FindAsync(record.Id);

            if (existingRecord == null)
            {
                throw new CSVWorkerArgumentException($"Record with ID {record.Id} not found for update.");
            }

            // Update properties
            existingRecord.PartNumber = record.PartNumber;
            existingRecord.ArticleName = record.ArticleName;
            existingRecord.MaterialGroup = record.MaterialGroup;
            existingRecord.CrossSec = record.CrossSec;
            existingRecord.LastUpdatedAt = DateTime.UtcNow;
            existingRecord.LastUpdatedBy = username;

            _context.IMDSPorscheDatabase.Update(existingRecord);
            await _context.SaveChangesAsync();

            return existingRecord;
        }

        // Offset-pagination with cancellation and basic validation.
        public async Task<PagedResult<IMDSPorscheDatabaseRecord>> GetPagedAsync(int? pageNumber, string? query)
        {
            var pageSize = _config.DefaultPageSize;

            if (pageNumber == null || pageNumber < 1) pageNumber = 1;

            IQueryable<IMDSPorscheDatabaseRecord> q = _context.IMDSPorscheDatabase;

            if (!string.IsNullOrWhiteSpace(query))
            {
                q = q.Where(r =>
                    (r.PartNumber != null && r.PartNumber.Contains(query)) ||
                    (r.ArticleName != null && r.ArticleName.Contains(query)) ||
                    (r.MaterialGroup != null && r.MaterialGroup.Contains(query)) ||
                    (r.CrossSec != null && r.CrossSec.Contains(query)));
            }

            var total = await q.CountAsync();
            var items = await q
                .OrderByDescending(r => r.LastUpdatedAt) // apply ordering after filtering
                .Skip((pageNumber.Value - 1) * pageSize)
                .Take(pageSize)
                .ToListAsync();

            return new PagedResult<IMDSPorscheDatabaseRecord>(items, total, pageNumber.Value, pageSize);
        }

        // Export Database
        public async Task<byte[]> ExportDatabaseAsync(CancellationToken cancellationToken)
        {
            var allRecords = await _context.IMDSPorscheDatabase.ToListAsync(cancellationToken);

            var exportedDatabase = new List<string[]>
            {
                (new string[] { "Item- /Mat.-No.", "Article Name", "FORS Material Group", "Cross-Sec (INDIV1)", "date" })
            };

            foreach (var record in allRecords)
            {
                exportedDatabase.Add(new string[]
                {
                    record.PartNumber ?? string.Empty,
                    record.ArticleName ?? string.Empty,
                    record.MaterialGroup ?? string.Empty,
                    record.CrossSec ?? string.Empty,
                    string.Empty
                });
            }

            var outputBytes = await CsvHelper.ConvertListToCsv(exportedDatabase, ';');

            return outputBytes;
        }
    }

}
