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

            // Database headers indexes
            const int databaseLeoniPartIndex = 2;
            const int databaseNodeIdIndex = 0;

            // FORS Bom indexes
            const int forsBomPartNumberIndex = 1;
            const int forsBomMaterialGroupIndex = 3;
            const int forsBomWeightIndex = 11;
            const int forsBomQuantityIndex = 4;

            // RS (Semi-Components / Materials / Rohstoff)
            List<string> rsMaterialGroups = new List<string>
                {
                    "SCHL", "KFT", "LTG", "BAND", "KAB", "WUD",
                    "FOL", "PUH", "HFA", "GLU", "GRA", "HAERT", "HARZ", "LWL",
                    "SLTG", "LOE", "SCHRU", "DOK", "FFC", "ZINN", "GS", "REST"
                };
            // RC (Discrete Components / Rüst Component)
            List<string> rcMaterialGroups = new List<string>
                {
                    "CKONT", "GEH", "CLIP", "BLIND", "EDICH", "TUELL", "RELAY", "VERB",
                    "VKONT", "KB", "EBOX", "KK", "BUSB", "ELK", "LKONT", "SCH",
                    "SKAP", "VMECH", "BATKL", "SICH", "SWIT", "OKONT", "SKONT", "FPCB",
                    "ETI", "APT"
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

                // Skip header
                var header = await reader.ReadLineAsync(cancellationToken);
                if (string.IsNullOrEmpty(header))
                {
                    throw new InvalidDataException("Database CSV file header is invalid or empty.");
                }
                var delimiter = CsvHelper.DetectDelimiter(header);

                // Read the file line by line asynchronously
                while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                {
                    // skip lines that have single and double quotes from line
                    if (line.Contains("'") || line.Contains("\""))
                    {
                        continue;
                    }

                    var row = CsvHelper.ParseLine(line, delimiter);
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

                var leoniPart = row.Length > databaseLeoniPartIndex ? row[databaseLeoniPartIndex] : null;
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

                                    // Skip row that has "Overall total"
                                    if (row[1] == "Overall total")
                                        continue;

                                    // Cables and tapes
                                    if (rsMaterialGroups.Contains(row[forsBomMaterialGroupIndex]))
                                    {
                                        cablesAndTapes.Add(row);
                                    }
                                    // Connectors
                                    else if (rcMaterialGroups.Contains(row[forsBomMaterialGroupIndex]))
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
                        string nodeId = "#N/A"; // Default value if not found
                        if (databaseByLeoniPart.TryGetValue(partNumber, out var databaseRow))
                        {
                            nodeId = databaseRow.Count() > databaseNodeIdIndex ? databaseRow[databaseNodeIdIndex] : string.Empty;

                            // if row is found but nodeId is empty, we consider it as missing node, and set nodeId to "#N/A"
                            // Also add it to missing nodes list
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
                        }
                        // if row not found in database, we also consider it as missing node, and add to missing nodes list
                        else
                        {
                            if (!hasMissingNodes || !currentFileHasMissingNodes)
                            {
                                hasMissingNodes = true;
                                currentFileHasMissingNodes = true;
                            }

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
                        string nodeId = "#N/A"; // Default value if not found
                        if (databaseByLeoniPart.TryGetValue(partNumber, out var databaseRow))
                        {
                            nodeId = databaseRow.Count() > 0 ? databaseRow[0] : string.Empty; // Assuming Node ID is in the first column (index 0)

                            // if row is found but nodeId is empty, we consider it as missing node, and set nodeId to "#N/A"
                            // Also add it to missing nodes list
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
                        }
                        else
                        {
                            if (!hasMissingNodes || !currentFileHasMissingNodes)
                            {
                                hasMissingNodes = true;
                                currentFileHasMissingNodes = true;
                            }


                            // Add to missing nodes list to be exported to "missing_nodes.csv"
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
            int lpcpLeoniPartIndex = -1;
            int lpcpForsPnIndex = -1;
            int lpcpSigipPnIndex = -1;
            int lpcpVisualPnIndex = -1;
            int lpcpWGKIndex = -1;

            // A2 headers indexes
            const int a2LPIndex = 0;
            const int a2NodeIdIndex = 1;

            // Load LPCP and A2 CSV files into memory as lists of string arrays (rows), using CsvHelper.ParseLine to split lines into values.

            // Load LPCP
            using (var stream = model.LPCPFile.OpenReadStream())
            using (var reader = new StreamReader(stream))
            {
                string? line;

                // Get header and determine column indexes for LEONI part number, FORS PN, SIGIP PN and Visual PN.
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
                        case "WGK":
                            lpcpWGKIndex = i;
                            break;
                    }
                }

                // throw error if critical column indexes are not found
                if (lpcpLeoniPartIndex == -1)
                {
                    throw new InvalidDataException("LPCP file is missing required column 'LEONI Part Number'.");
                }
                if (lpcpForsPnIndex == -1)
                {
                    throw new InvalidDataException("LPCP file is missing required column 'FORS Part Number'.");
                }
                if (lpcpSigipPnIndex == -1)
                {
                    throw new InvalidDataException("LPCP file is missing required column 'SIGIP Part Number'.");
                }
                if (lpcpVisualPnIndex == -1)
                {
                    throw new InvalidDataException("LPCP file is missing required column 'Visual Part Number'.");
                }
                if (lpcpWGKIndex == -1)
                {
                    throw new InvalidDataException("LPCP file is missing required column 'WGK'.");
                }

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

            // Number of lines parsed from LPCP
            _logger.LogInformation("UpdateDatabaseIMDS: Number of lines parsed from LPCP file: {LPCPLinesCount}", lpcpDoc.Count);

            using (var stream = model.A2File.OpenReadStream())
            using (var reader = new StreamReader(stream))
            {
                string? line;

                // Get header
                var headerLine = await reader.ReadLineAsync(cancellationToken);

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
                else if (headerRow.Length <= Math.Max(a2LPIndex, a2NodeIdIndex))
                {
                    throw new InvalidDataException("A2 header does not contain required columns for 'LP' and 'Noeud'.");
                }

                // Read the file line by line asynchronously
                while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                {
                    var row = CsvHelper.ParseLine(line, delimiter);
                    if (row != null)
                    {
                        // Handle special cases where line is like: "PXXX;PXXX";NODEIDXXX
                        if (row[0].Contains(";"))
                        {
                            var multiparts = row[0].Trim('\"').Trim().Split(';');
                            if (multiparts.Length > 0)
                            {
                                foreach (var part in multiparts)
                                {
                                    if (string.IsNullOrWhiteSpace(part))
                                    {
                                        continue; // Skip empty part numbers
                                    }
                                    a2Doc.Add(new string[] { part.Trim(), row.Length > 1 ? row[1] : string.Empty });
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
            var a2ByLP = new Dictionary<string, IReadOnlyList<string>>();
            foreach (var row in a2Doc)
            {
                // It is useful to check for cancellation inside long-running loops, 
                // especially when processing large files, to allow the operation to 
                // be cancelled gracefully if needed.
                cancellationToken.ThrowIfCancellationRequested();

                var LP = row.Length > a2LPIndex ? row[a2LPIndex] : null; // Assuming LP is in the first column (index 0)
                if (string.IsNullOrWhiteSpace(LP))
                {
                    continue;
                }

                // If there are duplicate LEONI part numbers, we keep the first one and ignore subsequent duplicates.
                if (!a2ByLP.ContainsKey(LP))
                {
                    a2ByLP[LP] = row;
                }
            }

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

                var partNumber = row.Length > lpcpLeoniPartIndex ? row[lpcpLeoniPartIndex] : string.Empty;
                var forsPn = row.Length > lpcpForsPnIndex ? row[lpcpForsPnIndex] : string.Empty;
                var sigipPn = row.Length > lpcpSigipPnIndex ? row[lpcpSigipPnIndex] : string.Empty;
                var visualPn = row.Length > lpcpVisualPnIndex ? row[lpcpVisualPnIndex] : string.Empty;
                var wgk = row.Length > lpcpWGKIndex ? row[lpcpWGKIndex] : string.Empty;

                // if all are empty, skip the row
                if (string.IsNullOrWhiteSpace(partNumber) && string.IsNullOrWhiteSpace(forsPn) && string.IsNullOrWhiteSpace(sigipPn) && string.IsNullOrWhiteSpace(visualPn) && string.IsNullOrWhiteSpace(wgk))
                {
                    continue;
                }

                var nodeId = string.Empty; // Default value if not found

                // Lookup for Node ID by part number
                if (!string.IsNullOrWhiteSpace(partNumber) && string.IsNullOrEmpty(nodeId) && a2ByLP.TryGetValue(partNumber, out var a2Row))
                {
                    if (a2Row == null || a2Row.Count < a2NodeIdIndex + 1)
                    {
                        continue; // Skip if A2 row is unexpectedly empty or missing Node ID column
                    }
                    nodeId = a2Row[a2NodeIdIndex];
                }

                // Lookup for Node ID by forsPn
                if (!string.IsNullOrWhiteSpace(forsPn) && string.IsNullOrEmpty(nodeId) && a2ByLP.TryGetValue(forsPn, out var a2Row2))
                {
                    if (a2Row2 == null || a2Row2.Count < a2NodeIdIndex + 1)
                    {
                        continue; // Skip if A2 row is unexpectedly empty or missing Node ID column
                    }
                    nodeId = a2Row2[a2NodeIdIndex];
                }

                // lookup for Node ID by sigipPn
                if (!string.IsNullOrWhiteSpace(sigipPn) && string.IsNullOrEmpty(nodeId) && a2ByLP.TryGetValue(sigipPn, out var a2Row3))
                {
                    if (a2Row3 == null || a2Row3.Count < a2NodeIdIndex + 1)
                    {
                        continue; // Skip if A2 row is unexpectedly empty or missing Node ID column
                    }
                    nodeId = a2Row3[a2NodeIdIndex];
                }

                // lookup for Node ID by visualPn
                if (!string.IsNullOrWhiteSpace(visualPn) && string.IsNullOrEmpty(nodeId) && a2ByLP.TryGetValue(visualPn, out var a2Row4))
                {
                    if (a2Row4 == null || a2Row4.Count < a2NodeIdIndex + 1)
                    {
                        continue; // Skip if A2 row is unexpectedly empty or missing Node ID column
                    }
                    nodeId = a2Row4[a2NodeIdIndex];
                }

                // looup for Node ID by wgk
                if (!string.IsNullOrWhiteSpace(wgk) && string.IsNullOrEmpty(nodeId) && a2ByLP.TryGetValue(wgk, out var a2Row5))
                {
                    if (a2Row5 == null || a2Row5.Count < a2NodeIdIndex + 1)
                    {
                        continue; // Skip if A2 row is unexpectedly empty or missing Node ID column
                    }
                    nodeId = a2Row5[a2NodeIdIndex];
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
            var databaseByLeoniPart = new Dictionary<string, IReadOnlyList<string>>();
            foreach (var row in database)
            {
                cancellationToken.ThrowIfCancellationRequested();

                var leoniPart = row.Length > databaseLeoniPartIndex ? row[databaseLeoniPartIndex] : null; // LEONI part number is in the third column (index 2)
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