namespace CSVWorker.Exceptions
{
    public class CSVWorkerInvalidDataException : CSVWorkerException
    {
        public CSVWorkerInvalidDataException()
        {
        }
        public CSVWorkerInvalidDataException(string message)
            : base(message)
        {
        }
        public CSVWorkerInvalidDataException(string message, Exception innerException)
            : base(message, innerException)
        {
        }
    }
}
