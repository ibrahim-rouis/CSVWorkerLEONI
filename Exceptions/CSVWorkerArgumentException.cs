namespace CSVWorker.Exceptions
{
    public class CSVWorkerArgumentException : CSVWorkerException
    {
        public CSVWorkerArgumentException()
        {
        }
        public CSVWorkerArgumentException(string message)
            : base(message)
        {
        }
        public CSVWorkerArgumentException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
