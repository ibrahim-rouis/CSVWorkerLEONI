using Microsoft.EntityFrameworkCore;
using CSVWorker.Models.Entities;

namespace CSVWorker.Models
{
    public class CSVWorkerDBContext : DbContext
    {
        public CSVWorkerDBContext(DbContextOptions<CSVWorkerDBContext> options) : base(options) { }

        public DbSet<User> Users { get; set; }
        public DbSet<Role> Roles { get; set; }

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            base.OnModelCreating(modelBuilder);

            // Configure relationships if needed
            modelBuilder.Entity<User>()
                .HasMany(u => u.Roles)
                .WithMany(u => u.Users);
        }
    }
}
