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

        /// <summary>
        /// This method transforms multiple FORS BOM CSV files into IMDS format CSV files, using a Database CSV file for enrichment.
        /// </summary>
        /// <param name="model">The view model containing the FORS BOM CSV files and the Database CSV file.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A byte array representing the transformed IMDS CSV files.</returns>
        /// <exception cref="ArgumentException">Thrown when input files are missing or invalid.</exception>
        /// <exception cref="InvalidDataException">Thrown when the Database CSV file header is invalid or empty.</exception>
        public async Task<byte[]> MultiForsBomToIMDS(MultiForsBomToIMDSBomVM model, CancellationToken cancellationToken)
        {
            _logger.LogInformation("MultiForsBomToIMDS started. CSV files count={CsvFilesCount}, Database CSV provided={DatabaseCsvName}", model.CsvFiles?.Count() ?? 0, model.DatabaseCSV?.FileName);

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
                   "BAND", "GRA", "FOL", "GLU", "HAERT", "HARZ", "SLTG", "LOE", "SCHRU", "LTG", "ZINN", "SCHL"
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
            {
                using (var reader = new StreamReader(stream))
                {
                    /** Read header **/
                    var header = await reader.ReadLineAsync(cancellationToken);
                    if (string.IsNullOrEmpty(header))
                    {
                        throw new InvalidDataException("Database CSV file header is invalid or empty.");
                    }

                    // detect delimiter (it is always ';' for leoni but sometimes it can be ','
                    var delimiter = CsvHelper.DetectDelimiter(header);

                    // split header line into columns
                    var headerRow = CsvHelper.ParseLine(header, delimiter);
                    if (headerRow == null || headerRow.Length == 0)
                    {
                        throw new InvalidDataException("Database CSV file header is invalid or empty.");
                    }

                    // Get required columns indexes by header names,
                    // with some fallback options for different possible header names,
                    // and fallback index if none of the expected header names are found.
                    databaseLeoniPartIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "LEONI Part Number", "PART/ITEM NO/" }, fallbackIndex: 2);
                    databaseNodeIdIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Node ID", "Noeud", "Nœud" }, fallbackIndex: 0);
                    databaseFORSPNIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "FORS PN", "FORS" }, fallbackIndex: 3);
                    databaseSIGIPNIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "SIGIP PN", "SIGIP" }, fallbackIndex: 4);
                    databaseVisualPNIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Visual PN", "Visual" }, fallbackIndex: 5);
                    databaseWGKIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "WGK", "Wide Group Key" }, fallbackIndex: 6);

                    /** Finish read header **/

                    // Read the file line by line asynchronously
                    string? line;
                    while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                    {
                        // Parse each line into columns using the detected delimiter and add to database list
                        var row = CsvHelper.ParseLine(line, delimiter);
                        if (row != null)
                        {
                            database.Add(row);
                        }
                    }
                }
            }

            // Build a fast lookup dictionary for the database by part number
            // These are needed to efficiently find Node IDs for the parts in the FORS BOM files when building the IMDS output.
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
                    // We will generate one IMDS CSV file per product number,
                    // so we need to keep track of the product numbers in the current FORS BOM file.
                    var productNumbers = new HashSet<string>();

                    /** Materials **/

                    // We will separate RS materials (cables, tapes, etc) from RC materials (discrete components like connectors)

                    // RS materials
                    var cablesAndTapes = new Dictionary<string, List<string[]>>();
                    // RC materials
                    var rcMaterials = new Dictionary<string, List<string[]>>();

                    /** End Materials **/

                    // Parse FORS BOM CSV
                    using (var stream = file.OpenReadStream())
                    {
                        using (var reader = new StreamReader(stream))
                        {
                            // skip fist 12 (0 -> 11) lines of the file as they usually contain metadata and other non-tabular information,
                            // and the actual tabular data usually starts from line index 12 after the header "Partnumber;Description;...".
                            for (int i = 0; i < 12; i++)
                            {
                                await reader.ReadLineAsync(cancellationToken);
                            }

                            // 3. Read the file line by line asynchronously
                            string? line;
                            while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                            {
                                // These FORS CSV files always use ';' as a delimiter
                                var row = CsvHelper.ParseLine(line, ';');
                                if (row != null && row.Length >= 12)
                                {
                                    // Product number is always in the first column
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
                    }

                    /** IMDS Output Construction **/

                    foreach (var productNumber in productNumbers)
                    {
                        _logger.LogInformation("Processing product number {ProductNumber} from file {FileName} to IMDS output", productNumber, file.FileName);

                        // current file has missing nodes
                        bool currentFileHasMissingNodes = false;

                        // Build IMDS

                        // Beginning of IMDS is always the same, with only product number changing in the second row.
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

                                // if nodeId is null (not found) add it to missing nodes list,
                                // and mark that current file has missing nodes so we can indicate it in the generated file name later.
                                if (nodeId == null)
                                {
                                    if (!currentFileHasMissingNodes)
                                    {
                                        currentFileHasMissingNodes = true;
                                    }
                                    missingNodes.Add([partNumber, "#N/A"]);
                                }

                                // Append to IMDS output
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
                if (missingNodes.Count > 1)
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

            _logger.LogInformation("MultiForsBomToIMDS finished. Generated {Count} BOM files inside ZIP.", model.CsvFiles.Count());
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

            // LPCP headers indexes.
            // we will determine the actual indexes dynamically at runtime by reading the header row.
            int lpcpLeoniPartIndex;
            int lpcpForsPnIndex;
            int lpcpSigipPnIndex;
            int lpcpVisualPnIndex;
            int lpcpWGKIndex;

            // A2 headers indexes
            // we will determine the actual indexes dynamically at runtime by reading the header row.
            int a2LPIndex;
            int a2NodeIdIndex;

            /** Load LPCP and A2 CSV files **/

            /** Load LPCP **/
            using (var stream = model.LPCPFile.OpenReadStream())
            {
                using (var readerRaw = new StreamReader(stream))
                {
                    // Read the entire LPCP CSV content as a string.
                    var csvString = await readerRaw.ReadToEndAsync(cancellationToken);

                    // Normalize the CSV string to ensure no row is split into multiple lines
                    // due to embedded line breaks within quoted fields.
                    var normalizedCsvString = CsvHelper.NormalizeCsvString(csvString);

                    // Use StringReader to read the normalized CSV string line by line.
                    using var normalizedReader = new StringReader(normalizedCsvString);

                    /** Read header **/

                    // Get header and determine column indexes for LEONI part number, FORS PN, SIGIP PN and Visual PN.
                    var headerLine = await normalizedReader.ReadLineAsync(cancellationToken);

                    if (string.IsNullOrEmpty(headerLine))
                    {
                        throw new InvalidDataException("LPCP file header is invalid or empty.");
                    }

                    // Detect delimiter (it is usually ';' but we want to be sure, and handle cases where it can be ',').
                    var delimiter = CsvHelper.DetectDelimiter(headerLine);

                    // Split header line into columns using the detected delimiter. 
                    var headerRow = CsvHelper.ParseLine(headerLine, delimiter);
                    if (headerRow == null || headerRow.Length == 0)
                    {
                        throw new InvalidDataException("LPCP file header is invalid or empty.");
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
                    while ((line = await normalizedReader.ReadLineAsync(cancellationToken)) != null)
                    {
                        // Parse each line into columns using the detected delimiter and add to lpcpDoc list
                        var row = CsvHelper.ParseLine(line, delimiter);
                        if (row != null)
                        {
                            lpcpDoc.Add(row);
                        }

                    }
                }
            }

            /** END Load LPCP **/

            // Number of lines parsed from LPCP
            _logger.LogInformation("UpdateDatabaseIMDS: Number of lines parsed from LPCP file: {LPCPLinesCount}", lpcpDoc.Count);

            /** Load A2 **/
            using (var stream = model.A2File.OpenReadStream())
            {
                using (var reader = new StreamReader(stream))
                {
                    // Read the entire LPCP CSV content as a string.
                    var csvString = await reader.ReadToEndAsync(cancellationToken);

                    // Normalize the CSV string to ensure no row is split into multiple lines
                    // due to embedded line breaks within quoted fields.
                    var normalizedCsvString = CsvHelper.NormalizeCsvString(csvString);

                    // Use StringReader to read the normalized CSV string line by line.
                    using var normalizedReader = new StringReader(normalizedCsvString);

                    /** Read header **/

                    // Get header
                    var headerLine = await normalizedReader.ReadLineAsync(cancellationToken);
                    if (string.IsNullOrEmpty(headerLine))
                    {
                        throw new InvalidDataException("A2 file header is invalid or empty.");
                    }

                    // Detect delimiter (it is usually ';' but we want to be sure, and handle cases where it can be ',').
                    var delimiter = CsvHelper.DetectDelimiter(headerLine);

                    // Split header line into columns using the detected delimiter.
                    var headerRow = CsvHelper.ParseLine(headerLine, delimiter);
                    if (headerRow == null || headerRow.Length == 0)
                    {
                        throw new InvalidDataException("A2 file header is invalid or empty.");
                    }

                    // Get required column indexes by header names.
                    a2LPIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "LP", "PART/ITEM NO/", "PART/ITEM NO/.", "LEONI Part Number" });
                    a2NodeIdIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Noeud", "Nœud", "Node ID" });

                    // Read the file line by line asynchronously
                    string? line;
                    while ((line = await normalizedReader.ReadLineAsync(cancellationToken)) != null)
                    {
                        // Split line into columns using the detected delimiter
                        var row = CsvHelper.ParseLine(line, delimiter);

                        if (row != null)
                        {
                            var a2LPValue = CsvHelper.TryGetValue(row, a2LPIndex);

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
                            /** END Handle special cases where line is like: "PXXX;PXXX";NODEIDXXX **/
                            else
                            {
                                // Normal case, just add the row as is.
                                a2Doc.Add(row);
                            }
                        }
                    }
                }
            }

            /** END Load A2 **/

            /** End Load LPCP and A2 CSV files **/

            // Number of lines parsed from A2
            _logger.LogInformation("UpdateDatabaseIMDS: Number of lines parsed from A2 file: {A2LinesCount}", a2Doc.Count);

            // Build fast lookup for A2 rows by LP
            // This is used to find Node IDs for the parts in the LPCP file when building the output.
            var a2ByLP = CsvHelper.BuildFastLookupDictionary(a2Doc, a2LPIndex);

            // Database output: first row only for titles, then rebuilt rows from A2 + LPCP enrichment
            var outputRows = new List<string[]>
            {
                (["Node ID", "x", "PART/ITEM NO/", "FORS PN", "SIGIP PN", "Visual PN", "WGK", "last Status Date", "Weight", "Weight Unit"])
            };

            // Iterate over LPCP rows and enrich with Node ID from A2, then add to output.
            foreach (var row in lpcpDoc)
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (row == null || row.Length == 0)
                {
                    continue; // Skip empty rows
                }

                // Get part number and other relevant fields from LPCP row using the determined indexes.
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

                /** Find Node ID **/

                var nodeId = string.Empty; // Default value if not found

                // We try to find the corresponding A2 row by multiple keys
                // in this order: part number, FORS PN, SIGIP PN, Visual PN, WGK.

                // Try to find A2 row by part number first
                var rowWithNodeIdFromA2 = a2ByLP!.GetValueOrDefault(partNumber, null);

                // If not found by part number, try by FORS PN
                if (rowWithNodeIdFromA2 == null)
                {
                    rowWithNodeIdFromA2 = a2ByLP!.GetValueOrDefault(forsPn, null);
                }
                // If not found by FORS PN, try by SIGIP PN
                if (rowWithNodeIdFromA2 == null)
                {
                    rowWithNodeIdFromA2 = a2ByLP!.GetValueOrDefault(sigipPn, null);
                }
                // If not found by SIGIP PN, try by Visual PN
                if (rowWithNodeIdFromA2 == null)
                {
                    rowWithNodeIdFromA2 = a2ByLP!.GetValueOrDefault(visualPn, null);
                }
                // If not found by Visual PN, try by WGK
                if (rowWithNodeIdFromA2 == null)
                {
                    rowWithNodeIdFromA2 = a2ByLP!.GetValueOrDefault(wgk, null);
                }

                // If found in A2, get Node ID from the row at the determined index.
                if (rowWithNodeIdFromA2 != null && rowWithNodeIdFromA2.Count > a2NodeIdIndex)
                {
                    nodeId = rowWithNodeIdFromA2[a2NodeIdIndex];
                }

                /** End find Node ID **/

                // Add row to output
                outputRows.Add([nodeId, string.Empty, partNumber, forsPn, sigipPn, visualPn, wgk, string.Empty, string.Empty, string.Empty]);
            }

            // Convert output rows to CSV bytes
            var outputCsvBytes = await CsvHelper.ConvertListToCsv(outputRows, ';', cancellationToken);

            _logger.LogInformation("UpdateDatabaseIMDS finished. Output rows={RowsCount}", outputRows.Count - 1);

            return outputCsvBytes;
        }

        // This method is intended to transform the IMDS BOM CSVs to Porsche IMDS CSVs, using the database for lookups as needed.
        public async Task<byte[]> IMDSBomToPorscheIMDS(IMDSBomToPorscheIMDS model, CancellationToken cancellationToken)
        {
            if (model.CsvFiles == null || !model.CsvFiles.Any() || model.DatabasePorscheCSV == null)
            {
                throw new ArgumentException("Input missing - please provide at least one FORS BOM CSV file and a Database CSV file.");
            }

            _logger.LogInformation("IMDSBomToPorscheIMDS started. Database file={DatabaseFileName}, Number of IMDS BOM files={BOMFilesCount}", model.DatabasePorscheCSV.FileName, model.CsvFiles.Count());

            // Porsche database indexes
            int databasePartNumberIndex;
            int databaseArticleNameIndex;
            int databaseCrossSecIndex;

            // IMDS BOM indexes
            const int imdsPartNumberIndex = 1;
            const int imdsQuantityIndex = 3;
            const int imdsWeightIndex = 4;
            const int imdsTypeIndex = 7;
            const int imdsNodeIdIndex = 8;

            // Porsche IMDS CSV indexes
            const int porscheIMDCrossSecIndex = 11;
            const int porscheIMDSnotApplicableIndex = 30;
            const int porscheIMDSDescriptionIndex = 2;
            (int Row, int Column) porscheIMDSWeightTotalIndex = (2, 4);


            /** Load Porsche Database **/
            var database = new List<string[]>();
            using (var stream = model.DatabasePorscheCSV.OpenReadStream())
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
                        throw new InvalidDataException("Database file header is invalid or empty.");
                    }

                    // Detect delimiter (it is usually ';' but we want to be sure, and handle cases where it can be ',').
                    var delimiter = CsvHelper.DetectDelimiter(headerLine);

                    // split header line into columns
                    var headerRow = CsvHelper.ParseLine(headerLine, delimiter);
                    if (headerRow == null || headerRow.Length == 0)
                    {
                        throw new InvalidDataException("Database CSV file header is invalid or empty.");
                    }


                    // Get required column indexes by header names.
                    databasePartNumberIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Item- /Mat.-No.", "PART/ITEM NO/", "PART/ITEM NO/.", "LEONI Part Number" });
                    databaseArticleNameIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Article Name", "Description" });
                    databaseCrossSecIndex = CsvHelper.GetRequiredColumnIndex(headerRow, new[] { "Cross-Sec (INDIV1)", "Cross-Sec" });

                    /** Finish reading header **/

                    // Read the file line by line asynchronously and add to database list
                    string? line;
                    while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                    {
                        var row = CsvHelper.ParseLine(line, delimiter);
                        if (row != null)
                        {
                            database.Add(row);
                        }
                    }
                }
            }

            /** Finish loading Porsche Database **/

            // Build a fast lookup dictionary for the database by part number,
            // to be used later when processing the IMDS BOM files.
            var databaseByPartNumber = CsvHelper.BuildFastLookupDictionary(database, databasePartNumberIndex);

            // Missing articles
            // Will be exported to "missing_articles.csv"
            var missingArticles = new List<string[]>
            {
                (["PART/ITEM NO/", "Article Name"])
            };


            // Create a ZIP archive in memory and add the generated CSV files as an entry.
            // The ZIP file will contain one CSV file with the IMDS data, 
            // and if there are missing nodes, another CSV file with the missing nodes data.
            var zipStream = new MemoryStream();
            using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Create, true))
            {
                /** Start processing IMDS BOM files **/
                foreach (var file in model.CsvFiles)
                {
                    if (!CsvHelper.IsValidCSV(file))
                    {
                        throw new ArgumentException($"File '{file.FileName}' is not a valid CSV file.");
                    }

                    /** Parse IMDS BOM CSV **/
                    var imdsCsvRows = new List<string[]>();
                    using (var stream = file.OpenReadStream())
                    {
                        using (var reader = new StreamReader(stream))
                        {
                            string? line;

                            // Read first line just to detect delimiter
                            line = await reader.ReadLineAsync(cancellationToken);
                            if (line == null)
                            {
                                _logger.LogWarning("File {FileName} is empty. Skipping this file.", file.FileName);
                                continue;
                            }
                            var delimiter = CsvHelper.DetectDelimiter(line, fallback: ',');

                            var frow = CsvHelper.ParseLine(line, delimiter);
                            if (frow != null)
                            {
                                IMDSHelper.ResizeAndFillRows(ref frow, 31);
                                imdsCsvRows.Add(frow);
                            }


                            while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                            {
                                var row = CsvHelper.ParseLine(line, delimiter);
                                if (row != null)
                                {
                                    // Porsche IMDS BOM have 31 columns
                                    IMDSHelper.ResizeAndFillRows(ref row, 31);
                                    imdsCsvRows.Add(row);
                                }
                            }
                        }
                    }
                    /** END Parse IMDS BOM CSV **/

                    /** Validate IMDS BOM structure **/
                    if (imdsCsvRows[0][0] != "MDS_BEGIN")
                    {
                        _logger.LogWarning("File {FileName} does not have valid IMDS BOM structure: first cell is not 'MDS_BEGIN'. Skipping this file.", file.FileName);
                        continue;
                    }

                    if (imdsCsvRows.Count < 5)
                    {
                        _logger.LogWarning("File {FileName} does not have valid IMDS BOM structure: it has less than 5 rows. Skipping this file.", file.FileName);
                        continue;
                    }

                    // Check if a row has #N/A as node ID
                    // Skip file if it has missing nodes
                    bool hasMissingNodes = false;
                    foreach (var row in imdsCsvRows)
                    {
                        if (row[imdsNodeIdIndex] == "#N/A")
                        {
                            hasMissingNodes = true;
                            break;
                        }
                    }
                    if (hasMissingNodes)
                    {
                        _logger.LogWarning("File {FileName} has missing nodes (Node ID is #N/A in some rows). Skipping this file.", file.FileName);
                        continue;
                    }
                    else
                    {
                        _logger.LogInformation("File {FileName} has valid IMDS BOM structure. Processing this file.", file.FileName);
                    }

                    /** END validate IMDS BOM structure **/

                    /** Trasnform to Porsche IMDS CSV **/

                    // get the product number
                    var productNumber = imdsCsvRows[2][0];

                    bool currentFileHasMissingArticles = false;

                    // Put cross-sec and article name for every row of type RS
                    foreach (var row in imdsCsvRows)
                    {
                        // Semi-components (RS) have cross-sec in the database,
                        // so we try to find it and add to the row, if type is RS.
                        if (row[imdsTypeIndex] == "RS")
                        {
                            var partNumber = row[1];

                            // Partnumber found in porsche database
                            if (databaseByPartNumber.TryGetValue(partNumber, out var dbrow))
                            {
                                // Set description (if not found , set #N/A# to be able to identify missing articles later in the output)
                                var articleName = dbrow[databaseArticleNameIndex];
                                if (!string.IsNullOrEmpty(articleName))
                                {
                                    row[porscheIMDSDescriptionIndex] = articleName;
                                }
                                else
                                {
                                    if (!currentFileHasMissingArticles)
                                    {
                                        currentFileHasMissingArticles = true;
                                    }
                                    missingArticles.Add([partNumber, "#N/A#"]);
                                }

                                // Set cross-sec (it will be removed later)
                                row[porscheIMDCrossSecIndex] = dbrow[databaseCrossSecIndex]; // Cross-Sec column
                            }
                            else
                            {
                                currentFileHasMissingArticles = true;
                                row[porscheIMDSDescriptionIndex] = "#N/A#";
                                missingArticles.Add([partNumber, "#N/A#"]);
                            }
                        }
                    }


                    /** Weights aggregation based on cross-sec + remove duplicates **/

                    /** 
                     * We go for each row that has RS as type, 
                     * look for other materials who have same cross-sec and aggregate the weights
                     * remove those rows and keep only keep the first one with the same cross-sec
                     * **/

                    // First aggregate weights based on cross-sec
                    var totalWeight = 0.0;
                    var totalWeightbyCrossSec = new Dictionary<string, double>();
                    foreach (var row in imdsCsvRows)
                    {
                        if (row[imdsTypeIndex] == "RS" && double.TryParse(row[imdsWeightIndex].Replace(',', '.'), out var weight))
                        {
                            var crossSec = row[porscheIMDCrossSecIndex];
                            totalWeight += weight;

                            if (string.IsNullOrEmpty(crossSec) || crossSec == "#N/A")
                            {
                                continue;
                            }

                            if (!totalWeightbyCrossSec.ContainsKey(crossSec))
                            {
                                totalWeightbyCrossSec[crossSec] = 0;
                            }
                            if (weight > 0.0)
                            {
                                totalWeightbyCrossSec[crossSec] += weight;
                            }
                        }
                    }

                    // set total weight in the specific cell for Porsche IMDS output
                    imdsCsvRows[porscheIMDSWeightTotalIndex.Row][porscheIMDSWeightTotalIndex.Column] = totalWeight.ToString("F3");

                    // Remove duplicates with same cross-sec, keep only first one, 
                    // and assign the aggregated weight.
                    var seenCrossSecs = new HashSet<string>();
                    for (int i = 0; i < imdsCsvRows.Count; i++)
                    {
                        var row = imdsCsvRows[i];

                        if (row[imdsTypeIndex] == "RS")
                        {
                            var crossSec = row[porscheIMDCrossSecIndex];

                            // Skip if crossSec is #N/A or empty
                            if (string.IsNullOrEmpty(crossSec) || crossSec == "#N/A")
                            {
                                continue;
                            }

                            // HashSet.Add returns true if it was added (first time seeing it), 
                            // and false if it already exists (it's a duplicate)
                            if (seenCrossSecs.Add(crossSec))
                            {
                                // Update its weight to the aggregated total we calculated earlier
                                if (totalWeightbyCrossSec.TryGetValue(crossSec, out var totw))
                                {
                                    // Make sure it matches the IMDS comma-decimal format
                                    row[imdsWeightIndex] = totw.ToString("F3");
                                }
                            }
                            else
                            {
                                // This is a duplicate. Remove it.
                                imdsCsvRows.RemoveAt(i);

                                // Decrement 'i' so we don't skip the next item that just shifted left into index 'i'
                                i--;
                            }
                        }
                    }

                    // remove all cross-sec for all RS rows
                    foreach (var row in imdsCsvRows)
                    {
                        if (!string.IsNullOrEmpty(row[porscheIMDCrossSecIndex]))
                        {
                            row[porscheIMDCrossSecIndex] = string.Empty;
                        }
                    }

                    // duplicate RS rows
                    // First one become C as type with 1 as quantity
                    // second one becomes a child
                    for (int i = 0; i < imdsCsvRows.Count; i++)
                    {
                        var row = imdsCsvRows[i];
                        if (row[imdsTypeIndex] == "RS")
                        {
                            var partnumber = row[imdsPartNumberIndex];

                            // Duplicate the row
                            var duplicatedRow = (string[])row.Clone();

                            row[imdsTypeIndex] = "C";
                            row[imdsQuantityIndex] = "1";
                            row[imdsPartNumberIndex] = "_" + partnumber;
                            row[imdsNodeIdIndex] = string.Empty;
                            row[porscheIMDSnotApplicableIndex] = "NotApplicable";

                            duplicatedRow[0] = "_" + partnumber;
                            duplicatedRow[1] = partnumber;
                            duplicatedRow[2] = partnumber;
                            duplicatedRow[imdsQuantityIndex] = string.Empty;

                            imdsCsvRows.Insert(i + 1, duplicatedRow);
                            i++;
                        }
                    }

                    // remove 3 rows before last row, they are not needed for Porsche IMDS and create issues in the output
                    if (imdsCsvRows.Count > 5)
                    {
                        imdsCsvRows.RemoveAt(imdsCsvRows.Count - 2);
                        imdsCsvRows.RemoveAt(imdsCsvRows.Count - 2);
                        imdsCsvRows.RemoveAt(imdsCsvRows.Count - 2);
                    }

                    var imdsFileName = $"{productNumber}_output.csv";

                    // If there are missing articles for the current file,
                    // we change the file name to include "missing_articles" to make it easily identifiable.
                    if (currentFileHasMissingArticles)
                    {
                        imdsFileName = $"0000_missing_articles_{productNumber}_output.csv";
                    }

                    // Add the IMDS output CSV to the ZIP archive
                    var imdsFileEntry = archive.CreateEntry(imdsFileName);
                    await using (var entryStream = imdsFileEntry.Open())
                    {
                        // IMDS csv is commas separated.
                        // unlike FORS BOM input csv files which are semicolon separated.
                        var imdsFileEntryOutputBytes = await CsvHelper.ConvertListToCsv(imdsCsvRows, ',', cancellationToken);
                        await entryStream.WriteAsync(imdsFileEntryOutputBytes, cancellationToken);
                        await entryStream.FlushAsync(cancellationToken);
                    }

                    /** END Transform to Porsche IMDS CSV **/
                }
                /** End processing IMDS BOM files **/

                // If there are missing articles, we add another entry 
                // to the ZIP with the details of the missing articles.
                // The CSV file will be "missing_articles.csv" 
                // and will contain two columns: "PART/ITEM NO/" and "Node ID".
                if (missingArticles.Count > 1)
                {
                    // first remove rows with duplicate part numbers in missingArticles, keeping only the first occurrence.
                    missingArticles = missingArticles.DistinctBy(row => row[0]).ToList();

                    // Add missing_articles.csv to ZIP archive
                    var missingArticlesFileName = "missing_articles.csv";
                    var missingArticlesFileEntry = archive.CreateEntry(missingArticlesFileName);
                    await using (var entryStream2 = missingArticlesFileEntry.Open())
                    {
                        var missingArticlesOuputBytes = await CsvHelper.ConvertListToCsv(missingArticles, ';', cancellationToken);
                        await entryStream2.WriteAsync(missingArticlesOuputBytes, cancellationToken);
                        await entryStream2.FlushAsync(cancellationToken);
                    }
                }
            }

            // Reset stream position so the Controller can return it as a File
            zipStream.Position = 0;

            _logger.LogInformation("IMDSBomToPorsche finished.");
            return zipStream.ToArray();
        }

    }
}