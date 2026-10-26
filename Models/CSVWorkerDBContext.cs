using CSVWorker.Models.Entities;
using Microsoft.EntityFrameworkCore;

namespace CSVWorker.Models
{
    public class CSVWorkerDBContext : DbContext
    {
        // Default encoding const
        private const string DefaultEncoding = "latin1";

        public CSVWorkerDBContext(DbContextOptions<CSVWorkerDBContext> options) : base(options) { }

        public DbSet<IMDSDatabaseRecord> IMDSDatabase { get; set; }

        public DbSet<IMDSPorscheDatabaseRecord> IMDSPorscheDatabase { get; set; }

        public DbSet<User> Users { get; set; }
        public DbSet<Role> Roles { get; set; }

        protected override void OnModelCreating(ModelBuilder builder)
        {
            base.OnModelCreating(builder);

            // Apply latin1 charset to all string properties across all entities
            foreach (var entity in builder.Model.GetEntityTypes())
            {
                foreach (var property in entity.GetProperties())
                {
                    if (property.ClrType == typeof(string))
                    {
                        property.SetCharSet(DefaultEncoding);
                    }
                }
            }

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

            //
            // CreatedAt defaults
            //
            builder.Entity<User>().Property(u => u.CreatedAt).HasColumnType("timestamp").HasDefaultValueSql("CURRENT_TIMESTAMP").ValueGeneratedOnAdd();
            builder.Entity<Role>().Property(r => r.CreatedAt).HasColumnType("timestamp").HasDefaultValueSql("CURRENT_TIMESTAMP").ValueGeneratedOnAdd();
            builder.Entity<IMDSDatabaseRecord>().Property(t => t.CreatedAt).HasColumnType("timestamp").HasDefaultValueSql("CURRENT_TIMESTAMP").ValueGeneratedOnAdd();
            builder.Entity<IMDSPorscheDatabaseRecord>().Property(tt => tt.CreatedAt).HasColumnType("timestamp").HasDefaultValueSql("CURRENT_TIMESTAMP").ValueGeneratedOnAdd();

            //
            // UpdatedAt auto-update
            //
            builder.Entity<User>()
                .Property(t => t.LastUpdatedAt)
                .HasColumnType("timestamp")
                .HasDefaultValueSql("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
                .ValueGeneratedOnAddOrUpdate();

            builder.Entity<IMDSDatabaseRecord>()
                .Property(u => u.LastUpdatedAt)
                .HasColumnType("timestamp")
                .HasDefaultValueSql("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
                .ValueGeneratedOnAddOrUpdate();

            builder.Entity<IMDSPorscheDatabaseRecord>()
                .Property(u => u.LastUpdatedAt)
                .HasColumnType("timestamp")
                .HasDefaultValueSql("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")
                .ValueGeneratedOnAddOrUpdate();
        }
    }
}
