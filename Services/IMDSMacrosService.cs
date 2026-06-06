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
        private readonly IMDSPorscheDatabaseService _porscheService;

        public IMDSMacrosService(ILogger<IMDSMacrosService> logger, IMDSDatabaseService imdsService, IMDSPorscheDatabaseService imdsPorscheDatabaseService)
        {
            _logger = logger;
            _imdsService = imdsService;
            _porscheService = imdsPorscheDatabaseService;
        }

        /// <summary>
        /// Generates IMDS-compliant CSV files from the provided FORS BOM CSV files and returns them as a ZIP archive.
        /// </summary>
        /// <param name="model">The model containing the list of FORS BOM CSV files to process.</param>
        /// <param name="cancellationToken">A cancellation token to monitor for cancellation requests.</param>
        /// <returns>A byte array representing the ZIP archive containing the generated IMDS CSV files.</returns>
        /// <exception cref="CSVWorkerArgumentException">Thrown when the input model is missing or contains invalid CSV files.</exception>
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
            using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Update, true))
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
                            bool isValidForsBom = false;
                            // skip fist 12 (0 -> 11) lines of the file as they usually contain metadata and other non-tabular information,
                            // and the actual tabular data usually starts from line index 12 after the header "Partnumber;Description;...".
                            for (int i = 0; i < 12; i++)
                            {
                                var l = await reader.ReadLineAsync(cancellationToken);
                                if (l == null)
                                {
                                    // File has less than 12 lines, skipping this file as it's not in expected format.
                                    _logger.LogDebug("File '{FileName}' has less than 12 lines. Skipping this file.", file.FileName);
                                    isValidForsBom = false;
                                    break;
                                }
                                var row = CsvHelper.ParseLine(l, ';');
                                // check if it contains any metadata headers like "Plant", "List type", "Partnumber (from)", "Partnumber (to)", "Project no.", "Material class", "Price type", "Date of calculation"
                                if (row != null && row.Length > 0 && (row[0].Contains("Plant", StringComparison.OrdinalIgnoreCase)
                                    || row[0].Contains("List type", StringComparison.OrdinalIgnoreCase)
                                    || row[0].Contains("Partnumber (from)", StringComparison.OrdinalIgnoreCase)
                                    || row[0].Contains("Partnumber (to)", StringComparison.OrdinalIgnoreCase)
                                    || row[0].Contains("Project no.", StringComparison.OrdinalIgnoreCase)
                                    || row[0].Contains("Material class", StringComparison.OrdinalIgnoreCase)
                                    || row[0].Contains("Price type", StringComparison.OrdinalIgnoreCase)
                                    || row[0].Contains("Date of calculation", StringComparison.OrdinalIgnoreCase)))
                                {
                                    // This file has metadata headers, we can consider it a valid FORS BOM file and continue processing.
                                    isValidForsBom = true;
                                }
                            }

                            if (!isValidForsBom)
                            {
                                _logger.LogDebug("Invalid FORS BOM file {FileName}, skipping.", file.FileName);
                                continue;
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
                                        ProductNumber = row[forsBomProdNumberIndex].Trim(),
                                        PartNumber = row[forsBomPartNumberIndex].Trim(),
                                        MaterialClass = row[forsBomMaterialGroupIndex].Trim(),
                                        MU = row[forsBomMUIndex].Trim(),
                                        Quantity = (int)(double.TryParse(row[forsBomQuantityIndex].Trim().Replace(',', '.'), out var quantity) ? quantity : 0),
                                        Weight = double.TryParse(row[forsBomWeightIndex].Trim().Replace(',', '.'), out var weight) ? weight * 1000 : 0 // convert weight from kg to g, as IMDS expects weight in grams
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

                        // materials for current product
                        var productMaterials = materials.Where(m => m.ProductNumber == productNumber).ToList();

                        // Get Node ID for materials
                        foreach (var material in productMaterials)
                        {
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
                        }

                        // Aggregate materials that have same non null node ID
                        // - For materials that have a non-null NodeID we group by NodeID and sum Weight and Quantity.
                        // - Materials without NodeID remain as individual entries (they will be exported with NodeID = "#N/A").
                        var materialsWithNode = productMaterials
                            .Where(m => !string.IsNullOrWhiteSpace(m.NodeID))
                            .GroupBy(m => m.NodeID!.Trim())
                            .Select(g => new ForsMaterial
                            {
                                ProductNumber = productNumber,
                                PartNumber = g.First().PartNumber,
                                MaterialClass = g.First().MaterialClass,
                                MU = g.First().MU,
                                NodeID = g.Key,
                                Quantity = g.Sum(x => x.Quantity),
                                Weight = g.Sum(x => x.Weight)
                            }
                            )
                            .ToList();

                        var materialsWithoutNode = productMaterials
                            .Where(m => string.IsNullOrWhiteSpace(m.NodeID))
                            .ToList();

                        var aggregatedProductMaterials = materialsWithNode
                            .Concat(materialsWithoutNode)
                            .ToList();

                        // Build IMDS

                        // Beginning of IMDS is always the same, with only product number changing in the second row.
                        var outputRow = new List<string[]>{
                                (["MDS_BEGIN", "Datasheet", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]),
                                ([string.Empty, productNumber, productNumber, string.Empty, string.Empty, "g", "5", string.Empty, string.Empty, string.Empty, string.Empty]),
                                ([productNumber, "CABLES & TAPES", "CABLES & TAPES", "1", string.Empty, "g", "5", "C", string.Empty, string.Empty, string.Empty])
                            };

                        // Get RS and RC materials for the current product number
                        var rsMaterials = aggregatedProductMaterials.Where(m => m.MU != "ST").ToList();
                        var rcMaterials = aggregatedProductMaterials.Where(m => m.MU == "ST").ToList();


                        // Add cables and tapes to IMDS output
                        if (rsMaterials != null && rsMaterials.Count > 0)
                        {
                            foreach (var material in rsMaterials)
                            {
                                // Append to IMDS output
                                outputRow.Add(["CABLES & TAPES", material.PartNumber, material.PartNumber, string.Empty, material.Weight.ToString("F3"), "g", string.Empty, "RS", material.NodeID ?? "#N/A", string.Empty, string.Empty]);
                            }
                        }

                        // Add connectors to IMDS output
                        if (rcMaterials != null && rcMaterials.Count > 0)
                        {
                            foreach (var material in rcMaterials)
                            {
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

                // If archive is empty throw error
                if (archive.Entries.Count == 0)
                {
                    throw new CSVWorkerArgumentException("No valid FORS BOM files found in the input. Please provide at least one valid FORS BOM CSV file.");
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

            _logger.LogDebug("MultiForsBomToIMDS finished.");
            return zipStream.ToArray();
        }


        public async Task<byte[]> IMDSBomToPorscheIMDS(IMDSBomToPorscheIMDS model, CancellationToken cancellationToken)
        {
            if (model.CsvFiles == null || !model.CsvFiles.Any())
            {
                throw new CSVWorkerArgumentException("Input missing - please provide at least one FORS BOM CSV file and a Database CSV file.");
            }

            _logger.LogDebug("IMDSBomToPorscheIMDS started. Number of IMDS BOM files={BOMFilesCount}", model.CsvFiles.Count());


            // IMDS BOM indexes
            const int imdsPartNumberIndex = 1;
            const int imdsQuantityIndex = 3;
            const int imdsWeightIndex = 4;
            const int imdsTypeIndex = 7;
            const int imdsNodeIdIndex = 8;

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
            using (var archive = new ZipArchive(zipStream, ZipArchiveMode.Update, true))
            {
                /** Start processing IMDS BOM files **/
                foreach (var file in model.CsvFiles)
                {
                    if (!CsvHelper.IsValidCSV(file))
                    {
                        throw new CSVWorkerArgumentException($"File '{file.FileName}' is not a valid CSV file.");
                    }

                    /** Parse IMDS BOM CSV **/
                    var materials = new List<ImdsMaterial>();
                    var productNumber = string.Empty;
                    bool currentFileHasMissingArticles = false;

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
                            var delimiter = CsvHelper.DetectDelimiter(line, fallback: ';');

                            // FIRST line has to be "MDS_BEGIN" in the first cell for the file to be considered a valid IMDS BOM file.
                            var firstrow = CsvHelper.ParseLine(line, delimiter);
                            if (firstrow == null || firstrow.Length == 0 || firstrow[0] != "MDS_BEGIN")
                            {
                                _logger.LogDebug("File '{FileName}' is not a valid IMDS BOM file. First cell of the first line is not 'MDS_BEGIN'. Skipping this file.", file.FileName);
                                continue;
                            }

                            // Second line has the product number in the second cell
                            var secondLine = await reader.ReadLineAsync(cancellationToken);
                            if (secondLine == null)
                            {
                                continue;
                            }
                            var secondRow = CsvHelper.ParseLine(secondLine, delimiter);
                            if (secondRow == null || secondRow.Length <= 2 || string.IsNullOrWhiteSpace(secondRow[1]))
                            {
                                continue;
                            }

                            productNumber = secondRow[1];

                            while ((line = await reader.ReadLineAsync(cancellationToken)) != null)
                            {
                                var row = CsvHelper.ParseLine(line, delimiter);
                                if (row != null && (row[imdsTypeIndex] == "RS" || row[imdsTypeIndex] == "RC"))
                                {
                                    // Add to materials list
                                    materials.Add(new ImdsMaterial
                                    {
                                        PartNumber = row[imdsPartNumberIndex],
                                        Quantity = (int)(double.TryParse(row[imdsQuantityIndex], out var quantity) ? quantity : 0),
                                        Weight = double.TryParse(row[imdsWeightIndex], out var weight) ? weight : 0,
                                        ComponentType = row[imdsTypeIndex],
                                        NodeID = row[imdsNodeIdIndex]
                                    });
                                }
                            }
                        }
                        /** END Parse IMDS BOM CSV **/

                        // Check if a material has NodeID = "#N/A" and skip file processing
                        if (materials.Where(m => string.IsNullOrWhiteSpace(m.NodeID) || m.NodeID == "#N/A").Any())
                        {
                            _logger.LogDebug("File {FileName} has missing Node(s). Skipping", file.FileName);
                            continue;
                        }
                    }

                    /** Build IMDS **/

                    // Get RS and RC materials for the current product number
                    var rsMaterials = materials.Where(p => p.ComponentType == "RS").ToList();
                    var rcMaterials = materials.Where(p => p.ComponentType == "RC").ToList();

                    // Find Article name & Cross-Sec of each RS material
                    foreach (var material in rsMaterials)
                    {
                        var article = await _porscheService.FindArticle(material.PartNumber);
                        if (article != null && !string.IsNullOrWhiteSpace(article.ArticleName))
                        {
                            material.ArticleName = article.ArticleName;
                        }
                        else
                        {
                            currentFileHasMissingArticles = true;
                            missingArticles.Add([material.PartNumber, "#N/A"]);
                        }

                        if (article != null && !string.IsNullOrWhiteSpace(article.CrossSec))
                        {
                            material.CrossSec = article.CrossSec;
                        }
                    }

                    var rsWithCross = rsMaterials
                        .Where(m => !string.IsNullOrWhiteSpace(m.CrossSec))
                        .ToList();

                    var rsWithoutCross = rsMaterials
                        .Where(m => string.IsNullOrWhiteSpace(m.CrossSec))
                        .ToList();

                    // Group rs Materials that have same CrossSec and sum their weights
                    var groupedByCross = rsWithCross
                        .GroupBy(m => m.CrossSec!.Trim().ToUpperInvariant())
                        .Select(g => new ImdsMaterial
                        {
                            PartNumber = g.First().PartNumber,
                            Quantity = g.Sum(m => m.Quantity),
                            Weight = g.Sum(m => m.Weight),
                            ComponentType = "RS",
                            NodeID = g.First().NodeID,
                            ArticleName = g.First().ArticleName,
                            CrossSec = g.Key
                        })
                        .ToList();

                    // Combine grouped (by CrossSec) and ungrouped (CrossSec null/empty) materials
                    var groupedRsMaterials = groupedByCross
                        .Concat(rsWithoutCross)
                        .ToList();

                    // Get total weight of all RS materials
                    double totalWeight = groupedRsMaterials.Sum(m => m.Weight);

                    // Beginning of IMDS
                    var outputRows = new List<string[]>{
                            (["MDS_BEGIN", "Datasheet", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty,  string.Empty]),
                            ([string.Empty, productNumber, productNumber, string.Empty, string.Empty, "g", "5", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty,  string.Empty]),
                            ([productNumber, "CABLES & TAPES", "CABLES & TAPES", "1", totalWeight.ToString("F3"), "g", "5", "C", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]),
                        };

                    // RS materials output
                    if (groupedRsMaterials != null && groupedRsMaterials.Count > 0)
                    {
                        foreach (var rsMat in groupedRsMaterials)
                        {
                            outputRows.Add(["CABLES & TAPES", $"_{rsMat.PartNumber}", rsMat.ArticleName, "1", rsMat.Weight.ToString("F3"), "g", string.Empty, "C", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, "NotApplicable"]);
                            outputRows.Add([$"_{rsMat.PartNumber}", rsMat.PartNumber, rsMat.PartNumber, string.Empty, rsMat.Weight.ToString("F3"), "g", string.Empty, "RS", rsMat.NodeID, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);
                        }
                    }

                    // RC materials output
                    if (rcMaterials != null && rcMaterials.Count > 0)
                    {
                        foreach (var rcMat in rcMaterials)
                        {
                            outputRows.Add([productNumber, rcMat.PartNumber, rcMat.PartNumber, rcMat.Quantity.ToString(), string.Empty, string.Empty, string.Empty, "RC", rcMat.NodeID, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);
                        }
                    }

                    // MDS_END
                    outputRows.Add(["MDS_END", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);
                    outputRows.Add(["END", string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty, string.Empty]);

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
                        var imdsFileEntryOutputBytes = await CsvHelper.ConvertListToCsv(outputRows, ';', cancellationToken);
                        await entryStream.WriteAsync(imdsFileEntryOutputBytes, cancellationToken);
                        await entryStream.FlushAsync(cancellationToken);
                    }

                    /** END Transform to Porsche IMDS CSV **/
                }
                /** End processing IMDS BOM files **/

                // If archive is empty throw error
                if (archive.Entries.Count() == 0)
                {
                    throw new CSVWorkerArgumentException("No valid IMDS BOM files found in the input. Please provide at least one valid IMDS BOM CSV file that has no missing Nodes.");
                }

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
