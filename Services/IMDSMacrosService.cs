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
                    "SCHL", "LTG", "BAND", "WUD",
                    "FOL", "PUH", "HFA", "GLU", "GRA", "HAERT", "HARZ", "LWL",
                    "SLTG", "LOE", "SCHRU", "DOK", "FFC", "ZINN", "GS", "REST"
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

                    // Product numbers
                    var productNumbers = new HashSet<string>();

                    /** Materials **/
                    // RS materials
                    var cablesAndTapes = new Dictionary<string, List<string[]>>();
                    // RC materials
                    var rcMaterials = new Dictionary<string, List<string[]>>();

                    // Parse FORS BOM CSV
                    using (var stream = file.OpenReadStream())
                    using (var reader = new StreamReader(stream))
                    {
                        string? line;

                        // Skip header
                        await reader.ReadLineAsync(cancellationToken);


                        // skip fist 12 lines of the file as they usually contain metadata and other non-tabular information,
                        // and the actual tabular data usually starts from line 13 with the header "Partnumber;Description;...".
                        for (int i = 0; i < 11; i++)
                        {
                            await reader.ReadLineAsync(cancellationToken);
                        }

                        // 3. Read the file line by line asynchronously
                        while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                        {
                            // These FORS CSV files use ';' as a delimiter
                            var row = CsvHelper.ParseLine(line, ';');
                            if (row != null && row.Length >= 12)
                            {
                                var productNumber = row[0];
                                productNumbers.Add(productNumber);

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
                                    if (!cablesAndTapes.ContainsKey(productNumber))
                                    {
                                        cablesAndTapes[productNumber] = new List<string[]>();
                                    }
                                    cablesAndTapes[productNumber].Add(row);
                                }
                                // Discrete Components (RC)
                                else
                                {
                                    if (!rcMaterials.ContainsKey(productNumber))
                                    {
                                        rcMaterials[productNumber] = new List<string[]>();
                                    }
                                    rcMaterials[productNumber].Add(row);
                                }
                            }
                        }
                    }

                    /** IMDS Output Construction **/

                    foreach (var productNumber in productNumbers)
                    {
                        _logger.LogInformation("Processing product number {ProductNumber} from file {FileName} to IMDS output", productNumber, file.FileName);

                        // current file has missing nodes
                        bool currentFileHasMissingNodes = false;
                        // Build IMDS
                        var outputRow = new List<string[]>{
                                (["MDS_BEGIN", "Datasheet", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]),
                                ([string.Empty, productNumber, productNumber, string.Empty, string.Empty, "g", "5", string.Empty, string.Empty, string.Empty, string.Empty]),
                                ([productNumber, "CABLES & TAPES", "CABLES & TAPES", "1", string.Empty, "g", "5", "C", string.Empty, string.Empty, string.Empty])
                            };

                        // Add cables and tapes to IMDS output
                        if (cablesAndTapes.ContainsKey(productNumber))
                        {
                            foreach (var row in cablesAndTapes[productNumber])
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
                                var nodeId = IMDSHelper.GetNodeID(partNumber, databaseByLeoniPart, databaseByFORSPN, databaseBySIGIPN, databaseByVisualPN, databaseByWGK, databaseNodeIdIndex);

                                // if nodeId is null (not found)
                                if (nodeId == null)
                                {
                                    if (!currentFileHasMissingNodes)
                                    {
                                        currentFileHasMissingNodes = true;
                                    }
                                    missingNodes.Add([partNumber, "#N/A"]);
                                }


                                outputRow.Add(["CABLES & TAPES", partNumber, partNumber, string.Empty, weight, "g", string.Empty, "RS", nodeId ?? "#N/A", string.Empty, string.Empty]);
                            }
                        }

                        // Add connectors to IMDS output
                        if (rcMaterials.ContainsKey(productNumber))
                        {
                            foreach (var row in rcMaterials[productNumber])
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
                                var nodeId = IMDSHelper.GetNodeID(partNumber, databaseByLeoniPart, databaseByFORSPN, databaseBySIGIPN, databaseByVisualPN, databaseByWGK, databaseNodeIdIndex);

                                // if nodeId is null (not found)
                                if (nodeId == null)
                                {
                                    if (!currentFileHasMissingNodes)
                                    {
                                        currentFileHasMissingNodes = true;
                                    }
                                    missingNodes.Add([partNumber, "#N/A"]);
                                }

                                outputRow.Add([productNumber, partNumber, partNumber, quantity, string.Empty, string.Empty, string.Empty, "RC", nodeId ?? "#N/A", string.Empty, string.Empty]);
                            }
                        }

                        // MDS_END
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
                    }
                    /** End processing FORS BOM files **/
                }

                // If there are missing nodes, we add another entry 
                // to the ZIP with the details of the missing nodes.
                // The CSV file will be "missing_nodes.csv" 
                // and will contain two columns: "PART/ITEM NO/" and "Node ID".
                if (missingNodes.Count > 0)
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

        public /*async*/ Task<byte[]> UpdateDatabasePorsche(UpdateDatabasePorscheVM model, CancellationToken cancellationToken)
        {
            _logger.LogInformation("UpdateDatabasePorsche started. LPCP file={LPCPFileName}", model.LPCPFile?.FileName);

            if (model.LPCPFile == null)
            {
                throw new NullReferenceException("Input must not be null. Usually it must be validated in controller before sending to service for processing.");
            }

            throw new NotImplementedException("The logic to update Porsche database is not implemented yet");
        }

        public /*async*/ Task<(string fileName, byte[] outputBytes)> IMDSBomToPorscheIMDS(IMDSBomToPorscheIMDS model, CancellationToken cancellationToken)
        {
            if (model.IMDSFileCSV == null || model.DatabasePorscheCSV == null)
            {
                throw new ArgumentException("Input missing - please provide the IMDS CSV file and the Database Porsche CSV file.");
            }

            // Here we would implement the logic to transform the IMDS BOM data to Porsche IMDS format,
            // using the database for lookups as needed. For now, we will just throw a NotImplementedException
            throw new NotImplementedException("The logic to transform IMDS BOM to Porsche IMDS is not implemented yet.");
        }
    }
}