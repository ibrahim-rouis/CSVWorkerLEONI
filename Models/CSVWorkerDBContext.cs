using CSVWorker.Models.Entities;
using Microsoft.EntityFrameworkCore;

namespace CSVWorker.Models
{
    public class CSVWorkerDBContext : DbContext
    {
        public CSVWorkerDBContext(DbContextOptions<CSVWorkerDBContext> options) : base(options)
        {
            // Force the connection open so the setting applies to the session
            Database.OpenConnection();

            // 2. Set the desired limit (e.g., 500 MB limit which is 128000 pages in SQLite, 1 page = 4096 bytes)
            Database.ExecuteSqlRaw("PRAGMA max_page_count = 128000;");

            // Performance tuning
            Database.ExecuteSqlRaw("PRAGMA journal_mode = WAL;");
            Database.ExecuteSqlRaw("PRAGMA synchronous = NORMAL;");
            Database.ExecuteSqlRaw("PRAGMA temp_store = MEMORY;");
            Database.ExecuteSqlRaw("PRAGMA cache_size = -64000;");
            Database.ExecuteSqlRaw("PRAGMA mmap_size = 268435456;");
            Database.ExecuteSqlRaw("PRAGMA busy_timeout = 5000;");
        }

        public DbSet<IMDSDatabaseRecord> IMDSDatabase { get; set; }

        public DbSet<IMDSPorscheDatabaseRecord> IMDSPorscheDatabase { get; set; }

        public DbSet<User> Users { get; set; }
        public DbSet<Role> Roles { get; set; }

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

            // Configure many-to-many relationship between User and Role
            builder.Entity<User>()
                .HasMany(u => u.Roles)
                .WithMany(u => u.Users);

            // CreatedAt Configurations
            builder.Entity<User>().Property(u => u.CreatedAt).HasColumnType("timestamp").HasDefaultValueSql("CURRENT_TIMESTAMP").ValueGeneratedOnAdd();
            builder.Entity<Role>().Property(u => u.CreatedAt).HasColumnType("timestamp").HasDefaultValueSql("CURRENT_TIMESTAMP").ValueGeneratedOnAdd();
            builder.Entity<IMDSDatabaseRecord>().Property(u => u.CreatedAt).HasColumnType("timestamp").HasDefaultValueSql("CURRENT_TIMESTAMP").ValueGeneratedOnAdd();
            builder.Entity<IMDSPorscheDatabaseRecord>().Property(u => u.CreatedAt).HasColumnType("timestamp").HasDefaultValueSql("CURRENT_TIMESTAMP").ValueGeneratedOnAdd();
        }
    }
}
