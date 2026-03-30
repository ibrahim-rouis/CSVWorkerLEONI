using CSVWorker.Services.CSVHelper;
using CSVWorker.ViewModels.Macros1;
using System.Collections.Generic;
using static System.Runtime.InteropServices.JavaScript.JSType;

namespace CSVWorker.Services.Macros
{
    public class Macros1Service
    {
        private readonly ILogger<Macros1Service> _logger;
        private readonly ICsvHelperService _csvHelper;

        public Macros1Service(ILogger<Macros1Service> logger, ICsvHelperService csvHelper)
        {
            _logger = logger;
            _csvHelper = csvHelper;
        }

        //public async Task<MemoryStream> MultiForsBomToSingleIMDSBoms(MultiForsBomToSingleBomsVM model, CancellationToken cancellationToken)
        //{
        //    if (model.CsvFiles == null || !model.CsvFiles.Any())
        //    {
        //        throw new ArgumentException("Input must contain at least one CSV file.");
        //    }

        //    // 1. Read and combine all uploaded CSV files
        //    foreach (var file in model.CsvFiles)
        //    {
        //        await using var inputStream = file.OpenReadStream();

        //        // The VBA macro uses Semicolon:=True
        //        var doc = await _csvHelper.ReadAsync(inputStream, hasHeader: false, delimiter: ';', cancellationToken: cancellationToken);


        //        // Clear TUV & RUV
        //        var validRows = doc.Rows.Where(row => !(row[0].Contains("TUV") || row[0].Contains("RUV"))).ToList();


        //    }



        //}

        public async Task<byte[]> UpdateDatabase(UpdateDatabaseVM model, CancellationToken cancellationToken)
        {
            if (model.LPCPFile == null || model.A2File == null)
            {
                throw new NullReferenceException("Input must not be null. Usually it must be validated in controller before sending to service for processing.");
            }

            await using var lpcpInputStream = model.LPCPFile.OpenReadStream();
            await using var a2InputStream = model.A2File.OpenReadStream();

            var lpcpDoc = await _csvHelper.ReadAsync(lpcpInputStream, hasHeader: true, cancellationToken: cancellationToken);
            var a2Doc = await _csvHelper.ReadAsync(a2InputStream, hasHeader: true, cancellationToken: cancellationToken);

            // A2 columns
            var a2LpIndex = _csvHelper.FindHeaderIndex(a2Doc.Headers, "LP");
            var a2NodeIndex = _csvHelper.FindHeaderIndex(a2Doc.Headers, "Noeud", "Node", "Node ID");

            // LPCP columns
            var lpcpLeoniIndex = _csvHelper.FindHeaderIndex(lpcpDoc.Headers, "LEONI Part Number");
            var lpcpForsIndex = _csvHelper.FindHeaderIndex(lpcpDoc.Headers, "FORS Part Number");
            var lpcpSigipIndex = _csvHelper.FindHeaderIndex(lpcpDoc.Headers, "SIGIP Part Number");
            var lpcpVisualIndex = _csvHelper.FindHeaderIndex(lpcpDoc.Headers, "Visual Part Number");

            // Build fast lookup by LEONI part number
            var lpcpByLeoniPart = new Dictionary<string, IReadOnlyList<string>>(StringComparer.OrdinalIgnoreCase);
            foreach (var row in lpcpDoc.Rows)
            {
                var leoniPart = _csvHelper.GetValue(row, lpcpLeoniIndex);
                if (string.IsNullOrWhiteSpace(leoniPart))
                {
                    continue;
                }

                if (!lpcpByLeoniPart.ContainsKey(leoniPart))
                {
                    lpcpByLeoniPart[leoniPart] = row;
                }
            }

            // BOM output: first row only for titles, then rebuilt rows from A2 + LPCP enrichment
            var outputRows = new List<IEnumerable<string?>>
            {
                new[] { "Node ID", "x", "PART/ITEM NO/", "FORS PN", "SIGIP PN", "Visual PN", "WGK", "last Status Date", "Weight" }
            };

            // For each A2 row, find matching LPCP row by LP/LEONI part number,
            // then build output row with enriched data. If no match, just keep A2 data with empty enrichment.
            foreach (var a2Row in a2Doc.Rows)
            {
                var partItemNo = _csvHelper.GetValue(a2Row, a2LpIndex);
                var nodeId = _csvHelper.GetValue(a2Row, a2NodeIndex);

                var forsPn = string.Empty;
                var sigipPn = string.Empty;
                var visualPn = string.Empty;

                if (!string.IsNullOrWhiteSpace(partItemNo) && lpcpByLeoniPart.TryGetValue(partItemNo, out var lpcpRow))
                {
                    forsPn = _csvHelper.GetValue(lpcpRow, lpcpForsIndex);
                    sigipPn = _csvHelper.GetValue(lpcpRow, lpcpSigipIndex);
                    visualPn = _csvHelper.GetValue(lpcpRow, lpcpVisualIndex);
                }

                outputRows.Add(new[] { nodeId, string.Empty, partItemNo, forsPn, sigipPn, visualPn, string.Empty, string.Empty, string.Empty });
            }

            using var outputStream = new MemoryStream();
            await _csvHelper.WriteAsync(outputStream, outputRows, ',', cancellationToken);

            var outPutBytes = outputStream.ToArray();

            _logger.LogInformation("UpdateDatabase finished. Output rows={RowsCount}", outputRows.Count - 1);

            return outPutBytes;
        }
    }
}