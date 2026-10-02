using CSVWorker.Configuration;
using CSVWorker.Models;
using CSVWorker.Models.Entities;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace CSVWorker.Services
{
    public class RolesService
    {
        private readonly ILogger<RolesService> _logger;
        private readonly CSVWorkerDBContext _context;
        private readonly CSVWorkerConfig _config;

        public RolesService(ILogger<RolesService> logger, CSVWorkerDBContext context, IOptions<CSVWorkerConfig> config)
        {
            _logger = logger;
            _context = context;
            _config = config.Value;
        }

        // Get role by name
        public async Task<Role?> GetRoleByNameAsync(string roleName)
        {
            var role = await _context.Roles
                .Include(r => r.Users)
                .FirstOrDefaultAsync(r => r.Name == roleName);
            return role;
        }

        // Get role by id
        public async Task<Role?> GetRoleByIdAsync(long roleId)
        {
            var role = await _context.Roles
                .Include(r => r.Users)
                .FirstOrDefaultAsync(r => r.Id == roleId);
            return role;
        }

        // Save role to database
        public async Task<Role> SaveAsync(string roleName)
        {
            Role newRole = new Role
            {
                Name = roleName,
                CreatedAt = DateTime.UtcNow,
            };
            _context.Roles.Add(newRole);
            await _context.SaveChangesAsync();

            return newRole;
        }

        // Delete role from database
        public async Task<bool> DeleteAsync(long roleId)
        {
            var role = await _context.Roles.FindAsync(roleId);
            if (role == null)
            {
                _logger.LogWarning("Role with ID {RoleId} not found for deletion.", roleId);
                return false;
            }
            _context.Roles.Remove(role);
            await _context.SaveChangesAsync();
            return true;
        }

        // Paginated list of roles
        public async Task<PagedResult<Role>> GetPagedAsync(int? pageNumber, string? query)
        {
            var pageSize = _config.DefaultPageSize;

            if (pageNumber == null || pageNumber < 1) pageNumber = 1;

            IQueryable<Role> q = _context.Roles;

            if (!string.IsNullOrWhiteSpace(query))
            {
                q = q.Where(r => r.Name.StartsWith(query));
            }

            var total = await q.CountAsync();
            var items = await q
                .OrderByDescending(r => r.CreatedAt) // apply ordering after filtering
                .Skip((pageNumber.Value - 1) * pageSize)
                .Take(pageSize)
                .AsNoTracking()
                .ToListAsync();

            return new PagedResult<Role>(items, total, pageNumber.Value, pageSize);
        }

        // Get all roles
        public async Task<List<Role>> GetAllRolesAsync()
        {
            return await _context.Roles
                .OrderBy(r => r.Name)
                .ToListAsync();
        }
    }
}
