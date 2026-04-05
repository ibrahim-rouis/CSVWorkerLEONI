using CSVWorker.ViewModels.IMDSMacros;
using System.IO.Compression;
using CSVWorker.Libs;

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

            // Load Database CSV
            var database = new List<string[]>();
            using (var stream = model.DatabaseCSV.OpenReadStream())
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
            var databaseByLeoniPart = new Dictionary<string, IReadOnlyList<string>>();
            foreach (var row in database)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var leoniPart = row.Length > 2 ? row[2] : null; // LEONI part number is in the third column (index 2)
                if (string.IsNullOrWhiteSpace(leoniPart))
                {
                    continue;
                }

                // If there are duplicate LEONI part numbers, we keep the first one and ignore subsequent duplicates.
                if (!databaseByLeoniPart.ContainsKey(leoniPart))
                {
                    databaseByLeoniPart[leoniPart] = row;
                }
            }

            // Metadata
            string productNumber = string.Empty;

            // Materials
            var cablesAndTapes = new List<string[]>();
            var connectors = new List<string[]>();

            // Missing nodes
            // Will be exported to "missing_nodes.csv"
            var missingNodes = new List<string[]>
            {
                (["PART/ITEM NO/", "Node ID"])
            };
            bool hasMissingNodes = false;

            /** Start processing FORS BOM files **/
            foreach (var file in model.CsvFiles)
            {
                if (!CsvHelper.IsValidCSV(file))
                {
                    throw new ArgumentException($"File '{file.FileName}' is not a valid CSV file.");
                }

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
                                // Store product number
                                if (string.IsNullOrEmpty(productNumber))
                                {
                                    productNumber = row[1];
                                }
                                else if (productNumber != row[1])
                                {
                                    throw new InvalidDataException($"Multiple product numbers found in FORS BOM files. Previous: {productNumber}, Current: {row[1]}. Please ensure all FORS BOM files belong to the same product.");
                                }
                            }
                            else if (row.Length >= 12)
                            {
                                // Skip header
                                if (row[0] == "Partnumber")
                                    continue;

                                // Skip rows that has "Total"
                                if (row[1] == "Total")
                                    continue;

                                // Skip row that has "Overall total"
                                if (row[1] == "Overall total")
                                    continue;

                                // Cables and tapes have "BAND", "FOL", "LTG" or "SLTG" in column 4 (index 3)
                                if (row[3] is "BAND" or "FOL" or "LTG" or "SLTG")
                                {
                                    cablesAndTapes.Add(row);
                                }
                                // Connectors have "BLIND", "CKONT", "EDICH", "GEH" or "KB" in column 4 (index 3)
                                else if (row[3] is "BLIND" or "CKONT" or "EDICH" or "GEH" or "KB")
                                {
                                    connectors.Add(row);
                                }
                            }
                        }
                    }
                }
            }
            /** End processing FORS BOM files **/

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
                var partNumber = row[1];
                var weight = row[11].Replace(',', '.'); // IMDS expects '.' as decimal separator

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
                string nodeId = "#N/A"; // Default value if not found
                if (databaseByLeoniPart.TryGetValue(partNumber, out var databaseRow))
                {
                    nodeId = databaseRow.Count() > 0 ? databaseRow[0] : string.Empty; // Assuming Node ID is in the first column (index 0)
                }
                else
                {
                    if (!hasMissingNodes)
                        hasMissingNodes = true;

                    missingNodes.Add([partNumber, nodeId]);
                }


                outputRow.Add(["CABLES & TAPES", partNumber, partNumber, string.Empty, weight, "g", string.Empty, "RS", nodeId, string.Empty, string.Empty]);
            }

            // Add connectors to IMDS output
            foreach (var row in connectors)
            {
                var partNumber = row[1];
                var quantity = row[4].Replace(',', '.'); // IMDS expects '.' as decimal separator
                var quantityValue = int.MinValue;

                // convert quantity to int
                if (double.TryParse(quantity, out var quantityValueDouble))
                {
                    quantityValue = (int)quantityValueDouble;
                }
                // If parsing fails, column will have ERR to indicate error parsing.
                quantity = quantityValue > int.MinValue ? quantityValue.ToString() : "ERR";

                // Find Node ID for this part number from the Database CSV, if available
                string nodeId = "#N/A"; // Default value if not found
                if (databaseByLeoniPart.TryGetValue(partNumber, out var databaseRow))
                {
                    nodeId = databaseRow.Count() > 0 ? databaseRow[0] : string.Empty; // Assuming Node ID is in the first column (index 0)
                }
                else
                {
                    if (!hasMissingNodes)
                        hasMissingNodes = true;

                    // Add to missing nodes list to be exported to "missing_nodes.csv"
                    missingNodes.Add([partNumber, nodeId]);
                }

                outputRow.Add([productNumber, partNumber, partNumber, quantity, string.Empty, string.Empty, string.Empty, "RC", nodeId, string.Empty, string.Empty]);
            }

            // Finish IMDS output
            outputRow.Add(["MDS_END", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);
            outputRow.Add([string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);
            outputRow.Add([string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);
            outputRow.Add([string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);
            outputRow.Add(["END", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);

            /** End of IMDS Output Construction **/

            // Create a ZIP archive in memory and add the generated CSV files as an entry.
            // The ZIP file will contain one CSV file with the IMDS data, 
            // and if there are missing nodes, another CSV file with the missing nodes data.
            var zipStream = new MemoryStream();
            using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Create, true))
            {
                var imdsFileName = $"{productNumber}.csv";

                // If there are missing nodes we rename the file name 
                // to indicate that the data is incomplete, 
                // so the user knows to check the "missing_nodes.csv" for details.
                if (hasMissingNodes)
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

                // If there are missing nodes, we add another entry 
                // to the ZIP with the details of the missing nodes.
                // The CSV file will be "missing_nodes.csv" 
                // and will contain two columns: "PART/ITEM NO/" and "Node ID".
                if (hasMissingNodes)
                {
                    var missingNodesFileName = "missing_nodes.csv";
                    var missingNodesFileEntry = archive.CreateEntry(missingNodesFileName);
                    await using (var entryStream2 = missingNodesFileEntry.Open())
                    {
                        var missingNodesOuputBytes = await CsvHelper.ConvertListToCsv(missingNodes, ',', cancellationToken);
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

            int lpcpLeoniPartIndex = -1;
            int lpcpForsPnIndex = -1;
            int lpcpSigipPnIndex = -1;
            int lpcpVisualPnIndex = -1;

            int a2PartItemNoIndex = -1;
            int a2NodeIdIndex = -1;

            // Load LPCP and A2 CSV files into memory as lists of string arrays (rows), using CsvHelper.ParseLine to split lines into values.
            using (var stream = model.LPCPFile.OpenReadStream())
            using (var reader = new StreamReader(stream))
            {
                string? line;

                // Get header and determine column indexes for LEONI part number, FORS PN, SIGIP PN and Visual PN.
                var headerLine = await reader.ReadLineAsync(cancellationToken);
                if (!string.IsNullOrEmpty(headerLine))
                {
                    var headerRow = CsvHelper.ParseLine(headerLine, ',');
                    if (headerRow == null)
                    {
                        throw new InvalidDataException("LPCP file header is invalid or empty.");
                    }

                    for (int i = 0; i < headerRow.Length; i++)
                    {
                        switch (headerRow[i].Trim())
                        {
                            case "LEONI Part Number":
                                lpcpLeoniPartIndex = i;
                                break;
                            case "FORS Part Number":
                                lpcpForsPnIndex = i;
                                break;
                            case "SIGIP Part Number":
                                lpcpSigipPnIndex = i;
                                break;
                            case "Visual Part Number":
                                lpcpVisualPnIndex = i;
                                break;
                        }
                    }
                }

                // throw error if critical column indexes are not found
                if (lpcpLeoniPartIndex == -1)
                {
                    throw new InvalidDataException("LPCP file is missing required column 'LEONI Part No.'.");
                }
                if (lpcpForsPnIndex == -1)
                {
                    throw new InvalidDataException("LPCP file is missing required column 'FORS PN'.");
                }
                if (lpcpSigipPnIndex == -1)
                {
                    throw new InvalidDataException("LPCP file is missing required column 'SIGIP PN'.");
                }
                if (lpcpVisualPnIndex == -1)
                {
                    throw new InvalidDataException("LPCP file is missing required column 'Visual PN'.");
                }

                // Read data line by line asynchronously
                while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                {
                    var row = CsvHelper.ParseLine(line, ',');
                    if (row != null)
                    {
                        lpcpDoc.Add(row);
                    }
                }
            }

            using (var stream = model.A2File.OpenReadStream())
            using (var reader = new StreamReader(stream))
            {
                string? line;

                // Get header
                var headerLine = await reader.ReadLineAsync(cancellationToken);
                if (!string.IsNullOrEmpty(headerLine))
                {
                    var headerRow = CsvHelper.ParseLine(headerLine, ',');
                    if (headerRow == null)
                    {
                        throw new InvalidDataException("A2 file header is invalid or empty.");
                    }

                    for (int i = 0; i < headerRow.Length; i++)
                    {
                        switch (headerRow[i].Trim())
                        {
                            case "LP":
                                a2PartItemNoIndex = i;
                                break;
                            case "Noeud":
                                a2NodeIdIndex = i;
                                break;
                        }
                    }
                }

                // throw error if critical column indexes are not found
                if (a2PartItemNoIndex == -1)
                {
                    throw new InvalidDataException("A2 file is missing required column 'PART/ITEM NO/'.");
                }
                if (a2NodeIdIndex == -1)
                {
                    throw new InvalidDataException("A2 file is missing required column 'Node ID'.");
                }


                // Read the file line by line asynchronously
                while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                {
                    var row = CsvHelper.ParseLine(line, ',');
                    if (row != null)
                    {
                        a2Doc.Add(row);
                    }
                }
            }

            // Build fast lookup by LEONI part number
            var lpcpByLeoniPart = new Dictionary<string, IReadOnlyList<string>>();
            foreach (var row in lpcpDoc)
            {
                // It is useful to check for cancellation inside long-running loops, 
                // especially when processing large files, to allow the operation to 
                // be cancelled gracefully if needed.
                cancellationToken.ThrowIfCancellationRequested();

                var leoniPart = row.Length > lpcpLeoniPartIndex ? row[lpcpLeoniPartIndex] : null; // Assuming LEONI part number is in the first column (index 0)
                if (string.IsNullOrWhiteSpace(leoniPart))
                {
                    continue;
                }

                // If there are duplicate LEONI part numbers, we keep the first one and ignore subsequent duplicates.
                if (!lpcpByLeoniPart.ContainsKey(leoniPart))
                {
                    lpcpByLeoniPart[leoniPart] = row;
                }
            }

            // Database output: first row only for titles, then rebuilt rows from A2 + LPCP enrichment
            var outputRows = new List<string[]>
            {
                (["Node ID", "x", "PART/ITEM NO/", "FORS PN", "SIGIP PN", "Visual PN", "WGK", "last Status Date", "Weight", "Weight Unit"])
            };

            // For each A2 row, find matching LPCP row by LP/LEONI part number,
            // then build output row with enriched data. If no match, just keep A2 data with empty enrichment.
            foreach (var a2Row in a2Doc)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (a2Row == null || a2Row.Length == 0)
                {
                    continue; // Skip empty rows
                }

                var partItemNo = a2Row.Length > a2PartItemNoIndex ? a2Row[a2PartItemNoIndex] : null;
                var nodeId = a2Row.Length > a2NodeIdIndex ? a2Row[a2NodeIdIndex] : null;

                if (string.IsNullOrWhiteSpace(nodeId) || string.IsNullOrWhiteSpace(partItemNo))
                {
                    continue; // Skip rows with missing critical data
                }

                var forsPn = string.Empty;
                var sigipPn = string.Empty;
                var visualPn = string.Empty;

                if (!string.IsNullOrWhiteSpace(partItemNo) && lpcpByLeoniPart.TryGetValue(partItemNo, out var lpcpRow))
                {
                    if (lpcpRow == null || lpcpRow.Count == 0)
                    {
                        continue; // Skip if LPCP row is unexpectedly empty
                    }

                    forsPn = lpcpRow.Count > lpcpForsPnIndex ? lpcpRow[lpcpForsPnIndex] : string.Empty; // Assuming FORS PN is in the second column (index 1)
                    sigipPn = lpcpRow.Count > lpcpSigipPnIndex ? lpcpRow[lpcpSigipPnIndex] : string.Empty; // Assuming SIGIP PN is in the third column (index 2)
                    visualPn = lpcpRow.Count > lpcpVisualPnIndex ? lpcpRow[lpcpVisualPnIndex] : string.Empty; // Assuming Visual PN is in the fourth column (index 3)
                }

                outputRows.Add([nodeId, string.Empty, partItemNo, forsPn, sigipPn, visualPn, string.Empty, string.Empty, string.Empty, string.Empty]);
            }

            var outputCsvBytes = await CsvHelper.ConvertListToCsv(outputRows, ',', cancellationToken);

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
            int leoniPartIndex = 0;
            int articleNameIndex = -1;
            int forsMaterialGroupIndex = -1;
            int crossSecIndex = -1;

            // Load LPCP
            using (var stream = model.LPCPFile.OpenReadStream())
            using (var reader = new StreamReader(stream))
            {
                string? line;

                // Get header and determine column indexes for LEONI part number, article name, FORS material group and cross section.
                var headerLine = await reader.ReadLineAsync(cancellationToken);
                if (!string.IsNullOrEmpty(headerLine))
                {
                    var headerRow = CsvHelper.ParseLine(headerLine, ',');
                    if (headerRow == null)
                    {
                        throw new InvalidDataException("LPCP file header is invalid or empty.");
                    }

                    for (int i = 0; i < headerRow.Length; i++)
                    {
                        switch (headerRow[i].Trim())
                        {
                            case "Item- /Mat.-No.":
                                leoniPartIndex = i;
                                break;
                            case "Article Name":
                                articleNameIndex = i;
                                break;
                            case "FORS Material Group":
                                forsMaterialGroupIndex = i;
                                break;
                            case "Cross-Sec (INDIV1)":
                                crossSecIndex = i;
                                break;
                        }
                    }
                }

                // throw error if critical column indexes are not found
                if (leoniPartIndex == -1)
                {
                    throw new InvalidDataException("LPCP file is missing required column 'Item- /Mat.-No.'.");
                }
                if (articleNameIndex == -1)
                {
                    throw new InvalidDataException("LPCP file is missing required column 'Article Name'.");
                }

                // Read data line by line asynchronously
                while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                {
                    var row = CsvHelper.ParseLine(line, ',');
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

                var leoniPart = lpcpRow.Length > leoniPartIndex ? lpcpRow[leoniPartIndex] : string.Empty;
                var articleName = lpcpRow.Length > articleNameIndex ? lpcpRow[articleNameIndex] : string.Empty;
                var forsMaterialGroup = lpcpRow.Length > forsMaterialGroupIndex && forsMaterialGroupIndex != -1 ? lpcpRow[forsMaterialGroupIndex] : string.Empty;
                var crossSec = lpcpRow.Length > crossSecIndex && crossSecIndex != -1 ? lpcpRow[crossSecIndex] : string.Empty;

                if (string.IsNullOrWhiteSpace(leoniPart))
                {
                    continue; // Skip rows with missing critical data
                }

                outputRows.Add([leoniPart, articleName, forsMaterialGroup, crossSec, string.Empty]);
            }

            var outputCsvBytes = await CsvHelper.ConvertListToCsv(outputRows, ',', cancellationToken);

            _logger.LogInformation("UpdateDatabasePorsche finished. Output rows={RowsCount}", outputRows.Count - 1);

            return outputCsvBytes;
        }
    }
}