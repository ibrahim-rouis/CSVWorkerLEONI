using CSVWorker.Libs;
using CSVWorker.ViewModels.IMDSMacros;
using System.IO.Compression;

namespace CSVWorker.Services
{
    public class IMDSMacrosService
    {
        private readonly ILogger<IMDSMacrosService> _logger;

        public IMDSMacrosService(ILogger<IMDSMacrosService> logger)
        {
            _logger = logger;
        }

        public async Task<byte[]> MultiForsBomToIMDS(MultiForsBomToIMDSBomVM model, CancellationToken cancellationToken)
        {
            _logger.LogInformation("Import5 started. CSV files count={CsvFilesCount}, Database CSV provided={DatabaseCsvName}", model.CsvFiles?.Count() ?? 0, model.DatabaseCSV?.FileName);

            if (model.CsvFiles == null || !model.CsvFiles.Any() || model.DatabaseCSV == null)
            {
                throw new ArgumentException("Input missing - please provide at least one FORS BOM CSV file and a Database CSV file.");
            }

            // Database headers indexes
            int databaseLeoniPartIndex;
            int databaseNodeIdIndex;
            int databaseFORSPNIndex;
            int databaseSIGIPNIndex;
            int databaseVisualPNIndex;
            int databaseWGKIndex;


            // FORS Bom indexes
            const int forsBomPartNumberIndex = 1;
            const int forsBomMaterialGroupIndex = 3;
            const int forsBomWeightIndex = 11;
            const int forsBomQuantityIndex = 4;

            // RS (Semi-Components / Materials / Rohstoff)
            List<string> rsMaterialGroups = new List<string>
                {
                    "GRA", "FOL", "GLU", "HAERT", "HARZ", "SLTG", "LOE", "SCHRU", "LTG", "ZINN", "SCHL", "BAND"
                };
            // Ignored material groups that are not relevant for IMDS 
            List<string> ignoreMaterialGroups = new List<string>
                {
                    "TUV", "RUV"
                };

            // Indicators for rows to skip
            // "Partnumber" in column 0 usually indicates header.
            // "Total", "Summe", etc usually indicate summary rows that should be skipped.
            string[] skipIndicators = new[] { "Partnumber", "Total", "Overall total", "Summe", "Gesamtsumme" };

            // Missing nodes
            // Will be exported to "missing_nodes.csv"
            var missingNodes = new List<string[]>
            {
                (["PART/ITEM NO/", "Node ID"])
            };
            bool hasMissingNodes = false;

            // Load Database CSV
            var database = new List<string[]>();
            using (var stream = model.DatabaseCSV.OpenReadStream())
            using (var reader = new StreamReader(stream))
            {
                string? line;

                // read header
                var header = await reader.ReadLineAsync(cancellationToken);
                if (string.IsNullOrEmpty(header))
                {
                    throw new InvalidDataException("Database CSV file header is invalid or empty.");
                }
                var delimiter = CsvHelper.DetectDelimiter(header);

                var headerRow = CsvHelper.ParseLine(header, delimiter);
                if (headerRow == null || headerRow.Length == 0)
                {
                    throw new InvalidDataException("Database CSV file header is invalid or empty.");
                }

                databaseLeoniPartIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "LEONI Part Number", "PART/ITEM NO/" }, fallbackIndex: 2);
                databaseNodeIdIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Node ID", "Noeud", "Nœud" }, fallbackIndex: 0);
                databaseFORSPNIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "FORS PN", "FORS" }, fallbackIndex: 3);
                databaseSIGIPNIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "SIGIP PN", "SIGIP" }, fallbackIndex: 4);
                databaseVisualPNIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Visual PN", "Visual" }, fallbackIndex: 5);
                databaseWGKIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "WGK", "Wide Group Key" }, fallbackIndex: 6);


                // Read the file line by line asynchronously
                while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                {
                    var row = CsvHelper.ParseLine(line, delimiter);
                    if (row != null)
                    {
                        database.Add(row);
                    }
                }
            }

            // Build a fast lookup dictionary for the database by part number
            var databaseByLeoniPart = CsvHelper.BuildFastLookupDictionary(database, databaseLeoniPartIndex);
            var databaseByFORSPN = CsvHelper.BuildFastLookupDictionary(database, databaseFORSPNIndex);
            var databaseBySIGIPN = CsvHelper.BuildFastLookupDictionary(database, databaseSIGIPNIndex);
            var databaseByVisualPN = CsvHelper.BuildFastLookupDictionary(database, databaseVisualPNIndex);
            var databaseByWGK = CsvHelper.BuildFastLookupDictionary(database, databaseWGKIndex);

            // Create a ZIP archive in memory and add the generated CSV files as an entry.
            // The ZIP file will contain one CSV file with the IMDS data, 
            // and if there are missing nodes, another CSV file with the missing nodes data.
            var zipStream = new MemoryStream();
            using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Create, true))
            {

                /** Start processing FORS BOM files **/
                foreach (var file in model.CsvFiles)
                {
                    if (!CsvHelper.IsValidCSV(file))
                    {
                        throw new ArgumentException($"File '{file.FileName}' is not a valid CSV file.");
                    }

                    // Metadata
                    string productNumber = string.Empty;
                    /** Materials **/
                    // RS materials
                    var cablesAndTapes = new List<string[]>();
                    // RC materials
                    var rcMaterials = new List<string[]>();

                    // current file has missing nodes
                    bool currentFileHasMissingNodes = false;

                    // Parse FORS BOM CSV
                    using (var stream = file.OpenReadStream())
                    using (var reader = new StreamReader(stream))
                    {
                        string? line;

                        // Skip header
                        await reader.ReadLineAsync(cancellationToken);

                        // 3. Read the file line by line asynchronously
                        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                        {
                            // These CSV files use ';' as a delimiter
                            var row = CsvHelper.ParseLine(line, ';');
                            if (row != null)
                            {
                                if (row.Length >= 2 && row[0] == "Partnumber (from)")
                                {
                                    productNumber = row[1];
                                }
                                else if (row.Length >= 12)
                                {
                                    // Skip header or rows that has "Total", "Summe", etc
                                    if (skipIndicators.Contains(row[0]) || skipIndicators.Contains(row[1]))
                                        continue;

                                    // Ignore some material groups that are not relevant for IMDS,
                                    if (ignoreMaterialGroups.Contains(row[forsBomMaterialGroupIndex]))
                                    {
                                        // do nothing, ignore these material groups
                                        continue;
                                    }
                                    // Cables and tapes (RS)
                                    else if (rsMaterialGroups.Contains(row[forsBomMaterialGroupIndex]))
                                    {
                                        cablesAndTapes.Add(row);
                                    }
                                    // Discrete Components (RC)
                                    else
                                    {
                                        rcMaterials.Add(row);
                                    }
                                }
                            }
                        }
                    }

                    /** IMDS Output Construction **/

                    // Build IMDS
                    var outputRow = new List<string[]>
            {
                (["MDS_BEGIN", "Datasheet", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]),
                ([string.Empty, productNumber, productNumber, string.Empty, string.Empty, "g", "5", string.Empty, string.Empty, string.Empty, string.Empty]),
                ([productNumber, "CABLES & TAPES", "CABLES & TAPES", "1", string.Empty, "g", "5", "C", string.Empty, string.Empty, string.Empty])
            };

                    // Add cables and tapes to IMDS output
                    foreach (var row in cablesAndTapes)
                    {
                        var partNumber = row[forsBomPartNumberIndex];
                        var weight = row[forsBomWeightIndex].Replace(',', '.'); // IMDS expects '.' as decimal separator

                        // convert weight to double
                        if (double.TryParse(weight, out var weightValue))
                        {
                            weightValue *= 1000; // Convert kg to g
                            weight = weightValue.ToString("F3");
                        }
                        else
                        {
                            // If parsing fails, column will have ERR to indicate error parsing.
                            weight = "ERR";
                        }

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
                            if (!hasMissingNodes || !currentFileHasMissingNodes)
                            {
                                hasMissingNodes = true;
                                currentFileHasMissingNodes = true;
                            }

                            nodeId = "#N/A";
                            missingNodes.Add([partNumber, nodeId]);
                        }


                        outputRow.Add(["CABLES & TAPES", partNumber, partNumber, string.Empty, weight, "g", string.Empty, "RS", nodeId, string.Empty, string.Empty]);
                    }

                    // Add connectors to IMDS output
                    foreach (var row in rcMaterials)
                    {
                        var partNumber = row[forsBomPartNumberIndex];
                        var quantity = row[forsBomQuantityIndex].Replace(',', '.'); // IMDS expects '.' as decimal separator
                        var quantityValue = int.MinValue;

                        // convert quantity to int
                        if (double.TryParse(quantity, out var quantityValueDouble))
                        {
                            quantityValue = (int)quantityValueDouble;
                        }
                        // If parsing fails, column will have ERR to indicate error parsing.
                        quantity = quantityValue > int.MinValue ? quantityValue.ToString() : "ERR";

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
                            if (!hasMissingNodes || !currentFileHasMissingNodes)
                            {
                                hasMissingNodes = true;
                                currentFileHasMissingNodes = true;
                            }

                            nodeId = "#N/A";
                            missingNodes.Add([partNumber, nodeId]);
                        }

                        outputRow.Add([productNumber, partNumber, partNumber, quantity, string.Empty, string.Empty, string.Empty, "RC", nodeId, string.Empty, string.Empty]);
                    }

                    // 
                    outputRow.Add(["MDS_END", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);

                    // I don't know why the original macro leaves 3 empty rows before "END", 
                    // but to keep the output consistent with the original macro, 
                    // I will also add 3 empty rows before "END".
                    outputRow.Add([string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);
                    outputRow.Add([string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);
                    outputRow.Add([string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);

                    // END row
                    outputRow.Add(["END", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);

                    /** End of IMDS Output Construction **/

                    var imdsFileName = $"{productNumber}.csv";

                    // If there are missing nodes we rename the file name 
                    // to indicate that the data is incomplete, 
                    // so the user knows to check the "missing_nodes.csv" for details.
                    if (currentFileHasMissingNodes)
                    {
                        imdsFileName = $"000000000_Missing_Data_{productNumber}.csv";
                    }

                    // Add the IMDS output CSV to the ZIP archive
                    var imdsFileEntry = archive.CreateEntry(imdsFileName);
                    await using (var entryStream = imdsFileEntry.Open())
                    {
                        // IMDS csv is commas separated.
                        // unlike FORS BOM input csv files which are semicolon separated.
                        var imdsFileEntryOutputBytes = await CsvHelper.ConvertListToCsv(outputRow, ',', cancellationToken);
                        await entryStream.WriteAsync(imdsFileEntryOutputBytes, cancellationToken);
                        await entryStream.FlushAsync(cancellationToken);
                    }

                    // clean up for next file processing
                    cablesAndTapes.Clear();
                    rcMaterials.Clear();
                }
                /** End processing FORS BOM files **/

                // If there are missing nodes, we add another entry 
                // to the ZIP with the details of the missing nodes.
                // The CSV file will be "missing_nodes.csv" 
                // and will contain two columns: "PART/ITEM NO/" and "Node ID".
                if (hasMissingNodes)
                {
                    // first remove rows with duplicate part numbers in missingNodes, keeping only the first occurrence.
                    missingNodes = missingNodes.DistinctBy(row => row[0]).ToList();

                    // Add missing_nodes.csv to ZIP archive
                    var missingNodesFileName = "missing_nodes.csv";
                    var missingNodesFileEntry = archive.CreateEntry(missingNodesFileName);
                    await using (var entryStream2 = missingNodesFileEntry.Open())
                    {
                        var missingNodesOuputBytes = await CsvHelper.ConvertListToCsv(missingNodes, ';', cancellationToken);
                        await entryStream2.WriteAsync(missingNodesOuputBytes, cancellationToken);
                        await entryStream2.FlushAsync(cancellationToken);
                    }
                }
            }
            // Reset stream position so the Controller can return it as a File
            zipStream.Position = 0;

            _logger.LogInformation("Import5 finished. Generated {Count} BOM files inside ZIP.", model.CsvFiles.Count());
            return zipStream.ToArray();
        }

        public async Task<byte[]> UpdateDatabaseIMDS(UpdateDatabaseVM model, CancellationToken cancellationToken)
        {
            _logger.LogInformation("UpdateDatabase started. LPCP file={LPCPFileName}, A2 file={A2FileName}", model.LPCPFile?.FileName, model.A2File?.FileName);

            if (model.LPCPFile == null || model.A2File == null)
            {
                throw new NullReferenceException("Input must not be null. Usually it must be validated in controller before sending to service for processing.");
            }

            var lpcpDoc = new List<string[]>();
            var a2Doc = new List<string[]>();

            // LPCP headers indexes - we will determine the actual indexes dynamically at runtime by reading the header row.
            int lpcpLeoniPartIndex;
            int lpcpForsPnIndex;
            int lpcpSigipPnIndex;
            int lpcpVisualPnIndex;
            int lpcpWGKIndex;

            // A2 headers indexes
            int a2LPIndex;
            int a2NodeIdIndex;

            // Load LPCP and A2 CSV files into memory as lists of string arrays (rows), using CsvHelper.ParseLine to split lines into values.

            // Load LPCP
            using (var stream = model.LPCPFile.OpenReadStream())
            using (var readerRaw = new StreamReader(stream))
            {
                var csvString = await readerRaw.ReadToEndAsync(cancellationToken);

                // Normalize the CSV string to ensure no row is split into multiple lines due to embedded line breaks within quoted fields.
                var normalizedCsvString = CsvHelper.NormalizeCsvString(csvString);

                using var normalizedReader = new StringReader(normalizedCsvString);

                // Get header and determine column indexes for LEONI part number, FORS PN, SIGIP PN and Visual PN.
                var headerLine = await normalizedReader.ReadLineAsync(cancellationToken);

                if (string.IsNullOrEmpty(headerLine))
                {
                    throw new InvalidDataException("LPCP file header is invalid or empty.");
                }

                var delimiter = CsvHelper.DetectDelimiter(headerLine);

                var headerRow = CsvHelper.ParseLine(headerLine, delimiter);
                if (headerRow == null || headerRow.Length == 0)
                {
                    throw new InvalidDataException("LPCP file header is invalid or empty.");
                }

                lpcpLeoniPartIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "LEONI Part Number", "PART/ITEM NO/", "PART/ITEM NO/.", "Item- /Mat.-No." });
                lpcpForsPnIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "FORS Part Number" });
                lpcpSigipPnIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "SIGIP Part Number" });
                lpcpVisualPnIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Visual Part Number" });
                lpcpWGKIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "WGK" });

                // Read data line by line asynchronously
                string? line;
                while ((line = await normalizedReader.ReadLineAsync(cancellationToken)) != null)
                {
                    var row = CsvHelper.ParseLine(line, delimiter);

                    if (row != null)
                    {
                        lpcpDoc.Add(row);
                    }

                }
            }

            // Number of lines parsed from LPCP
            _logger.LogInformation("UpdateDatabaseIMDS: Number of lines parsed from LPCP file: {LPCPLinesCount}", lpcpDoc.Count);

            // Load A2
            using (var stream = model.A2File.OpenReadStream())
            using (var reader = new StreamReader(stream))
            {
                var csvString = await reader.ReadToEndAsync(cancellationToken);

                // Normalize the CSV string to ensure no row is split into multiple lines due to embedded line breaks within quoted fields.
                var normalizedCsvString = CsvHelper.NormalizeCsvString(csvString);
                using var normalizedReader = new StringReader(normalizedCsvString);

                // Get header
                var headerLine = await normalizedReader.ReadLineAsync(cancellationToken);

                if (string.IsNullOrEmpty(headerLine))
                {
                    throw new InvalidDataException("A2 file header is invalid or empty.");
                }

                var delimiter = CsvHelper.DetectDelimiter(headerLine);

                var headerRow = CsvHelper.ParseLine(headerLine, delimiter);
                if (headerRow == null || headerRow.Length == 0)
                {
                    throw new InvalidDataException("A2 file header is invalid or empty.");
                }

                a2LPIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "LP", "PART/ITEM NO/", "PART/ITEM NO/.", "LEONI Part Number" });
                a2NodeIdIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Noeud", "Nœud", "Node ID" });

                // Read the file line by line asynchronously
                string? line;

                while ((line = await normalizedReader.ReadLineAsync(cancellationToken)) != null)
                {
                    var row = CsvHelper.ParseLine(line, delimiter);
                    if (row != null)
                    {
                        // Handle special cases where line is like: "PXXX;PXXX";NODEIDXXX
                        var a2LPValue = CsvHelper.TryGetValue(row, a2LPIndex);
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
                                    var requiredLength = Math.Max(row.Length, Math.Max(a2LPIndex, a2NodeIdIndex) + 1);
                                    var normalizedRow = new string[requiredLength];

                                    // Optional: preserve other original columns.
                                    Array.Copy(row, normalizedRow, row.Length);

                                    normalizedRow[a2LPIndex] = trimmedPart;
                                    normalizedRow[a2NodeIdIndex] = CsvHelper.TryGetValue(row, a2NodeIdIndex) ?? string.Empty;

                                    a2Doc.Add(normalizedRow);
                                }
                            }
                        }
                        else
                        {
                            a2Doc.Add(row);
                        }
                    }
                }
            }

            // Number of lines parsed from A2
            _logger.LogInformation("UpdateDatabaseIMDS: Number of lines parsed from A2 file: {A2LinesCount}", a2Doc.Count);

            // Build fast lookup for A2 rows by LP
            var a2ByLP = CsvHelper.BuildFastLookupDictionary(a2Doc, a2LPIndex);

            // Database output: first row only for titles, then rebuilt rows from A2 + LPCP enrichment
            var outputRows = new List<string[]>
            {
                (["Node ID", "x", "PART/ITEM NO/", "FORS PN", "SIGIP PN", "Visual PN", "WGK", "last Status Date", "Weight", "Weight Unit"])
            };

            // For each A2 row, find matching LPCP row by LP/LEONI part number,
            // then build output row with enriched data. If no match, just keep A2 data with empty enrichment.
            foreach (var row in lpcpDoc)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (row == null || row.Length == 0)
                {
                    continue; // Skip empty rows
                }

                var partNumber = CsvHelper.TryGetValue(row, lpcpLeoniPartIndex) ?? string.Empty;
                var forsPn = CsvHelper.TryGetValue(row, lpcpForsPnIndex) ?? string.Empty;
                var sigipPn = CsvHelper.TryGetValue(row, lpcpSigipPnIndex) ?? string.Empty;
                var visualPn = CsvHelper.TryGetValue(row, lpcpVisualPnIndex) ?? string.Empty;
                var wgk = CsvHelper.TryGetValue(row, lpcpWGKIndex) ?? string.Empty;

                // if all are empty, skip the row
                if (string.IsNullOrWhiteSpace(partNumber) && string.IsNullOrWhiteSpace(forsPn) && string.IsNullOrWhiteSpace(sigipPn) && string.IsNullOrWhiteSpace(visualPn) && string.IsNullOrWhiteSpace(wgk))
                {
                    continue;
                }

                var nodeId = string.Empty; // Default value if not found

                var rowWithNodeIdFromA2 = a2ByLP!.GetValueOrDefault(partNumber, null); // Try to find A2 row by part number first
                if (rowWithNodeIdFromA2 == null)
                {
                    rowWithNodeIdFromA2 = a2ByLP!.GetValueOrDefault(forsPn, null); // If not found by part number, try by FORS PN
                }

                if (rowWithNodeIdFromA2 == null)
                {
                    rowWithNodeIdFromA2 = a2ByLP!.GetValueOrDefault(sigipPn, null); // If not found by FORS PN, try by SIGIP PN
                }

                if (rowWithNodeIdFromA2 == null)
                {
                    rowWithNodeIdFromA2 = a2ByLP!.GetValueOrDefault(visualPn, null); // If not found by SIGIP PN, try by Visual PN
                }

                if (rowWithNodeIdFromA2 == null)
                {
                    rowWithNodeIdFromA2 = a2ByLP!.GetValueOrDefault(wgk, null); // If not found by Visual PN, try by WGK
                }

                // If found in A2, get Node ID from the row
                if (rowWithNodeIdFromA2 != null && rowWithNodeIdFromA2.Count > a2NodeIdIndex)
                {
                    nodeId = rowWithNodeIdFromA2[a2NodeIdIndex];
                }

                outputRows.Add([nodeId, string.Empty, partNumber, forsPn, sigipPn, visualPn, wgk, string.Empty, string.Empty, string.Empty]);
            }

            var outputCsvBytes = await CsvHelper.ConvertListToCsv(outputRows, ';', cancellationToken);

            _logger.LogInformation("UpdateDatabaseIMDS finished. Output rows={RowsCount}", outputRows.Count - 1);

            return outputCsvBytes;
        }

        public async Task<byte[]> UpdateDatabasePorsche(UpdateDatabasePorscheVM model, CancellationToken cancellationToken)
        {
            _logger.LogInformation("UpdateDatabasePorsche started. LPCP file={LPCPFileName}", model.LPCPFile?.FileName);

            if (model.LPCPFile == null)
            {
                throw new NullReferenceException("Input must not be null. Usually it must be validated in controller before sending to service for processing.");
            }

            var lpcpDoc = new List<string[]>();

            int leoniPartIndex;
            int articleNameIndex;
            int forsMaterialGroupIndex;
            int crossSecIndex;

            // Load LPCP
            using (var stream = model.LPCPFile.OpenReadStream())
            using (var reader = new StreamReader(stream))
            {
                string? line;

                // Get header and determine column indexes for LEONI part number, article name, FORS material group and cross section.
                var headerLine = await reader.ReadLineAsync(cancellationToken);
                if (string.IsNullOrEmpty(headerLine))
                {
                    throw new InvalidDataException("LPCP file header is invalid or empty.");
                }

                var delimiter = CsvHelper.DetectDelimiter(headerLine);

                var headerRow = CsvHelper.ParseLine(headerLine, delimiter);
                if (headerRow == null || headerRow.Length == 0)
                {
                    throw new InvalidDataException("LPCP file header is invalid or empty.");
                }

                leoniPartIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "LEONI Part Number", "PART/ITEM NO/", "Item- /Mat.-No." });
                articleNameIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Article Name" });
                forsMaterialGroupIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "FORS Material Group" });
                crossSecIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Cross-Sec (INDIV1)" });

                // Read data line by line asynchronously
                while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                {
                    var row = CsvHelper.ParseLine(line, delimiter);
                    if (row != null)
                    {
                        lpcpDoc.Add(row);
                    }
                }
            }

            // Database output
            var outputRows = new List<string[]>
            {
                (["Item- /Mat.-No.", "Article Name", "FORS Material Group", "Cross-Sec (INDIV1)", "date"])
            };

            foreach (var lpcpRow in lpcpDoc)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (lpcpRow == null || lpcpRow.Length == 0)
                {
                    continue; // Skip empty rows
                }

                var leoniPart = CsvHelper.TryGetValue(lpcpRow, leoniPartIndex) ?? string.Empty;
                var articleName = CsvHelper.TryGetValue(lpcpRow, articleNameIndex) ?? string.Empty;
                var forsMaterialGroup = CsvHelper.TryGetValue(lpcpRow, forsMaterialGroupIndex) ?? string.Empty;
                var crossSec = CsvHelper.TryGetValue(lpcpRow, crossSecIndex) ?? string.Empty;

                if (string.IsNullOrWhiteSpace(leoniPart))
                {
                    continue; // Skip rows with missing critical data
                }

                outputRows.Add([leoniPart, articleName, forsMaterialGroup, crossSec, string.Empty]);
            }

            var outputCsvBytes = await CsvHelper.ConvertListToCsv(outputRows, ';', cancellationToken);

            _logger.LogInformation("UpdateDatabasePorsche finished. Output rows={RowsCount}", outputRows.Count - 1);

            return outputCsvBytes;
        }

        public async Task<(string fileName, byte[] outputBytes)> IMDSBomToPorscheIMDS(IMDSBomToPorscheIMDS model, CancellationToken cancellationToken)
        {
            if (model.IMDSFileCSV == null || model.DatabasePorscheCSV == null)
            {
                throw new ArgumentException("Input missing - please provide the IMDS CSV file and the Database Porsche CSV file.");
            }

            // Database headers indexes
            const int databaseLeoniPartIndex = 0;
            const int databaseArticleNameIndex = 1;

            // Load Database Porsche CSV
            var database = new List<string[]>();
            using (var stream = model.DatabasePorscheCSV.OpenReadStream())
            using (var reader = new StreamReader(stream))
            {
                string? line;

                // Skip header
                await reader.ReadLineAsync(cancellationToken);

                // Read the file line by line asynchronously
                while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                {
                    var row = CsvHelper.ParseLine(line, ',');
                    if (row != null)
                    {
                        database.Add(row);
                    }
                }
            }

            // Build a fast lookup dictionary for the database by part number
            var databaseByLeoniPart = CsvHelper.BuildFastLookupDictionary(database, databaseLeoniPartIndex);

            // return same IMDS CSV file bytes for now until further notice
            using (var stream = model.IMDSFileCSV.OpenReadStream())
            using (var reader = new StreamReader(stream))
            {
                string? line;
                var imdsData = new List<string[]>();

                // Read the file line by line asynchronously
                while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                {
                    var row = CsvHelper.ParseLine(line, ',');
                    if (row != null)
                    {
                        imdsData.Add(row);
                    }
                }

                // Here we would implement the logic to transform the IMDS BOM data to Porsche IMDS format,
                // using the database for lookups as needed. For now, we will just throw a NotImplementedException
                throw new NotImplementedException("The logic to transform IMDS BOM to Porsche IMDS is not implemented yet.");
            }
        }
    }
}