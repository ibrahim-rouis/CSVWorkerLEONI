using CSVWorker.Models;
using CSVWorker.Models.Entities;
using CSVWorker.Security;
using Microsoft.EntityFrameworkCore;

namespace CSVWorker.Data
{
    public static class DbInitializer
    {
        private static readonly string[] DefaultRoles = [Roles.AdminGroupName, Roles.ManagerGroupName];

        public static async Task InitializeAsync(IServiceProvider services, ILogger logger)
        {
            using var scope = services.CreateScope();
            var context = scope.ServiceProvider.GetRequiredService<CSVWorkerDBContext>();

            // Create the database if it does not exist and apply any pending migrations
            await context.Database.MigrateAsync();
            logger.LogInformation("Database migration completed.");

            // Seed default roles
            var existingRoles = await context.Roles
                .Where(r => DefaultRoles.Contains(r.Name))
                .Select(r => r.Name)
                .ToListAsync();

            var missingRoles = DefaultRoles.Except(existingRoles);
            foreach (var roleName in missingRoles)
            {
                context.Roles.Add(new Role
                {
                    Name = roleName,
                    CreatedAt = DateTime.UtcNow
                });
                logger.LogInformation("Seeded role: {RoleName}", roleName);
            }

            await context.SaveChangesAsync();
        }
    }
}