namespace CSVWorker.Exceptions
{
    public class CSVWorkerMissingColumnException: CSVWorkerException
    {
        public CSVWorkerMissingColumnException()
        {
        }
        public CSVWorkerMissingColumnException(string message)
            : base(message)
        {
        }
        public CSVWorkerMissingColumnException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
