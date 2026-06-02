using Microsoft.EntityFrameworkCore;

namespace CSVWorker.Models
{
    public class CSVWorkerDBContext : DbContext
    {
        public CSVWorkerDBContext(DbContextOptions<CSVWorkerDBContext> options) : base(options) { }
    }
}
