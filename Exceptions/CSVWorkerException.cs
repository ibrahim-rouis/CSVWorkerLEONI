namespace CSVWorker.Exceptions
{
    public class CSVWorkerException : Exception
    {
        public CSVWorkerException()
        {
        }

        public CSVWorkerException(string message)
            : base(message)
        {
        }

        public CSVWorkerException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
