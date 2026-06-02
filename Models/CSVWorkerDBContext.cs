using CSVWorker.Models.Entities;
using Microsoft.EntityFrameworkCore;

namespace CSVWorker.Models
{
    public class CSVWorkerDBContext : DbContext
    {
        public CSVWorkerDBContext(DbContextOptions<CSVWorkerDBContext> options) : base(options) { }

        public DbSet<IMDSDatabaseRecord> IMDSDatabase { get; set; }

        public DbSet<IMDSPorscheDatabaseRecord> IMDSPorscheDatabase { get; set; }

        protected override void OnModelCreating(ModelBuilder builder)
        {
            base.OnModelCreating(builder);

            builder.Entity<IMDSDatabaseRecord>(b =>
            {
                b.HasKey(e => e.Id);
                b.HasIndex(e => e.PartNumber);
                b.HasIndex(e => e.ForsPN);
                b.HasIndex(e => e.SIGIPPN);
                b.HasIndex(e => e.VisualPN);
                b.HasIndex(e => e.WGK);
                b.HasIndex(e => e.NodeID);
            });

            builder.Entity<IMDSPorscheDatabaseRecord>(b =>
            {
                b.HasKey(e => e.Id);
                b.HasIndex(e => e.PartNumber);
                b.HasIndex(e => e.ArticleName);
                b.HasIndex(e => e.MaterialGroup);
                b.HasIndex(e => e.CrossSec);
            });
        }
    }
}
