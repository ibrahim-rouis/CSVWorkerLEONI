using CSVWorker.Exceptions;
using CSVWorker.Libs;
using CSVWorker.Models.DTO;
using CSVWorker.Models.ViewModels.IMDSMacros;
using System.IO.Compression;

namespace CSVWorker.Services
{
    public class IMDSMacrosService
    {
        private readonly ILogger<IMDSMacrosService> _logger;
        private readonly IMDSDatabaseService _imdsService;

        public IMDSMacrosService(ILogger<IMDSMacrosService> logger, IMDSDatabaseService imdsService)
        {
            _logger = logger;
            _imdsService = imdsService;
        }

        /// <summary>
        /// This method transforms multiple FORS BOM CSV files into IMDS format CSV files, using a Database CSV file for enrichment.
        /// </summary>
        /// <param name="model">The view model containing the FORS BOM CSV files and the Database CSV file.</param>
        /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
        /// <returns>A byte array representing the transformed IMDS CSV files.</returns>
        /// <exception cref="CSVWorkerArgumentException">Thrown when input files are missing or invalid.</exception>
        /// <exception cref="CSVWorkerInvalidDataException">Thrown when the Database CSV file header is invalid or empty.</exception>
        public async Task<byte[]> MultiForsBomToIMDS(MultiForsBomToIMDSBomVM model, CancellationToken cancellationToken)
        {
            _logger.LogDebug("MultiForsBomToIMDS started. CSV files count={CsvFilesCount}", model.CsvFiles?.Count() ?? 0);

            if (model.CsvFiles == null || !model.CsvFiles.Any())
            {
                throw new CSVWorkerArgumentException("Input missing - please provide at least one FORS BOM CSV");
            }


            // FORS Bom indexes
            const int forsBomProdNumberIndex = 0;
            const int forsBomPartNumberIndex = 1;
            const int forsBomMaterialGroupIndex = 3;
            const int forsBomWeightIndex = 11;
            const int forsBomQuantityIndex = 4;
            const int forsBomMUIndex = 5;


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
                        throw new CSVWorkerArgumentException($"File '{file.FileName}' is not a valid CSV file.");
                    }
                    // RS materials
                    var materials = new List<ForsMaterial>();

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
                                    // Skip header or rows that has "Total", "Summe", etc
                                    if (skipIndicators.Contains(row[0]) || skipIndicators.Contains(row[1]))
                                        continue;

                                    // Ignore some material groups that are not relevant for IMDS,
                                    if (ignoreMaterialGroups.Contains(row[forsBomMaterialGroupIndex]))
                                    {
                                        // do nothing, ignore these material groups
                                        continue;
                                    }

                                    materials.Add(new ForsMaterial
                                    {
                                        ProductNumber = row[forsBomProdNumberIndex],
                                        PartNumber = row[forsBomPartNumberIndex],
                                        MaterialClass = row[forsBomMaterialGroupIndex],
                                        MU = row[forsBomMUIndex],
                                        Quantity = double.TryParse(row[forsBomQuantityIndex].Replace(',', '.'), out var quantity) ? quantity : 0,
                                        Weight = double.TryParse(row[forsBomWeightIndex].Replace(',', '.'), out var weight) ? weight * 1000 : 0 // convert weight from kg to g, as IMDS expects weight in grams
                                    });
                                }
                            }
                        }
                    }

                    /** IMDS Output Construction **/

                    // Get distinct product numbers from the materials, as we need to generate one IMDS file per product number.
                    var productNumbers = materials.Select(m => m.ProductNumber).Distinct();

                    foreach (var productNumber in productNumbers)
                    {
                        // current file has missing nodes
                        bool currentFileHasMissingNodes = false;

                        // Build IMDS

                        // Beginning of IMDS is always the same, with only product number changing in the second row.
                        var outputRow = new List<string[]>{
                                (["MDS_BEGIN", "Datasheet", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]),
                                ([string.Empty, productNumber, productNumber, string.Empty, string.Empty, "g", "5", string.Empty, string.Empty, string.Empty, string.Empty]),
                                ([productNumber, "CABLES & TAPES", "CABLES & TAPES", "1", string.Empty, "g", "5", "C", string.Empty, string.Empty, string.Empty])
                            };

                        // Get RS and RC materials for the current product number
                        var rsMaterials = materials.Where(m => m.ProductNumber == productNumber && m.MU != "ST").ToList();
                        var rcMaterials = materials.Where(m => m.ProductNumber == productNumber && m.MU == "ST").ToList();


                        // Add cables and tapes to IMDS output
                        if (rsMaterials != null && rsMaterials.Count > 0)
                        {
                            foreach (var material in rsMaterials)
                            {

                                // Find Node ID for this part number from the Database
                                material.NodeID = await _imdsService.FindNodeIDbyAny(material.PartNumber);

                                // if nodeId is null (not found) add it to missing nodes list,
                                // and mark that current file has missing nodes so we can indicate it in the generated file name later.
                                if (material.NodeID == null)
                                {
                                    if (!currentFileHasMissingNodes)
                                    {
                                        currentFileHasMissingNodes = true;
                                    }
                                    missingNodes.Add([material.PartNumber, "#N/A"]);
                                }

                                // Append to IMDS output
                                outputRow.Add(["CABLES & TAPES", material.PartNumber, material.PartNumber, string.Empty, material.Weight.ToString("F3"), "g", string.Empty, "RS", material.NodeID ?? "#N/A", string.Empty, string.Empty]);
                            }
                        }

                        // Add connectors to IMDS output
                        if (rcMaterials != null && rcMaterials.Count > 0)
                        {
                            foreach (var material in rcMaterials)
                            {
                                // Find Node ID for this part number from the Database
                                material.NodeID = await _imdsService.FindNodeIDbyAny(material.PartNumber);

                                // if nodeId is null (not found)
                                if (material.NodeID == null)
                                {
                                    if (!currentFileHasMissingNodes)
                                    {
                                        currentFileHasMissingNodes = true;
                                    }
                                    missingNodes.Add([material.PartNumber, "#N/A"]);
                                }

                                int quantity = (int)material.Quantity;

                                outputRow.Add([productNumber, material.PartNumber, material.PartNumber, quantity.ToString(), string.Empty, string.Empty, string.Empty, "RC", material.NodeID ?? "#N/A", string.Empty, string.Empty]);
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
                            var imdsFileEntryOutputBytes = await CsvHelper.ConvertListToCsv(outputRow, ';', cancellationToken);
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

            _logger.LogDebug("MultiForsBomToIMDS finished. Generated {Count} BOM files inside ZIP.", model.CsvFiles.Count());
            return zipStream.ToArray();
        }

        /// <summary>
        /// Transforms IMDS BOM data into Porsche IMDS format and generates a ZIP file containing the output CSV files.
        /// </summary>
        /// <param name="model">The model containing the input CSV files and the Porsche database CSV file.</param>
        /// <param name="cancellationToken">A cancellation token to monitor for cancellation requests.</param>
        /// <returns>A byte array representing the ZIP file containing the transformed IMDS data and any missing articles.</returns>
        /// <exception cref="CSVWorkerArgumentException">Thrown when the input is missing required CSV files.</exception>
        /// <exception cref="CSVWorkerInvalidDataException">Thrown when the database file header is invalid or empty.</exception>
        public async Task<byte[]> IMDSBomToPorscheIMDS(IMDSBomToPorscheIMDS model, CancellationToken cancellationToken)
        {
            if (model.CsvFiles == null || !model.CsvFiles.Any() || model.DatabasePorscheCSV == null)
            {
                throw new CSVWorkerArgumentException("Input missing - please provide at least one FORS BOM CSV file and a Database CSV file.");
            }

            _logger.LogDebug("IMDSBomToPorscheIMDS started. Database file={DatabaseFileName}, Number of IMDS BOM files={BOMFilesCount}", model.DatabasePorscheCSV.FileName, model.CsvFiles.Count());

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
                        throw new CSVWorkerArgumentException($"File '{file.FileName}' is not a valid CSV file.");
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
                                //File is empty. Skipping this file.
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
                        _logger.LogDebug("File {FileName} does not have valid IMDS BOM structure: first cell is not 'MDS_BEGIN'. Skipping this file.", file.FileName);
                        continue;
                    }

                    if (imdsCsvRows.Count < 5)
                    {
                        _logger.LogDebug("File {FileName} does not have valid IMDS BOM structure: it has less than 5 rows. Skipping this file.", file.FileName);
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
                        _logger.LogDebug("File {FileName} has missing nodes (Node ID is #N/A in some rows). Skipping this file.", file.FileName);
                        continue;
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