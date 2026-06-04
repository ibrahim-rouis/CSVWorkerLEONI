namespace CSVWorker.Libs
{
    public partial interface IMDSHelper
    {
        /// <summary>
        /// Resizes the row to the specified size and fills new cells with empty strings if the original row is shorter than the specified size.
        /// </summary>
        /// <param name="row">The row to resize.</param>
        /// <param name="size">The desired size of the row.</param>
        static void ResizeAndFillRows(ref string[] row, int size)
        {
            var originalLength = row.Length;

            Array.Resize(ref row, size);

            if (originalLength < size)
            {
                Array.Fill(row, string.Empty, originalLength, size - originalLength);
            }
        }
    }
}
