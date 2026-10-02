using CSVWorker.Configuration;
using CSVWorker.Exceptions;
using CSVWorker.Models;
using CSVWorker.Models.Entities;
using CSVWorker.Security;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;

namespace CSVWorker.Services
{
    public class UsersService
    {
        private readonly ILogger<UsersService> _logger;
        private readonly CSVWorkerDBContext _context;
        private readonly CSVWorkerConfig _config;
        private readonly RolesService _rolesService;

        public UsersService(ILogger<UsersService> logger, CSVWorkerDBContext context, IOptions<CSVWorkerConfig> config, RolesService rolesService)
        {
            _logger = logger;
            _context = context;
            _config = config.Value;
            _rolesService = rolesService;
        }

        // Get user by name
        public async Task<User?> GetUserByNameAsync(string userName)
        {
            var user = await _context.Users
                .Include(u => u.Roles)
                .FirstOrDefaultAsync(u => u.Name == userName);

            return user;
        }

        // Get user by id
        public async Task<User?> GetUserByIdAsync(long userId)
        {
            var user = await _context.Users
                .Include(u => u.Roles)
                .FirstOrDefaultAsync(u => u.Id == userId);
            return user;
        }

        // Check if user exists by name
        public async Task<bool> UserExistsAsync(string userName)
        {
            return await _context.Users.AnyAsync(u => u.Name == userName);
        }

        // Check if any user exists in the database
        public async Task<bool> AnyUsersAsync()
        {
            return await _context.Users.AnyAsync();
        }

        // Save user to database
        public async Task<User> SaveAsync(User user)
        {
            // Throw csvworker exception if user already exists
            if (await UserExistsAsync(user.Name))
            {
                throw new Exception($"User with name {user.Name} already exists.");
            }

            user.CreatedAt = DateTime.UtcNow;
            _context.Users.Add(user);
            await _context.SaveChangesAsync();

            return user;
        }

        // Save user to database
        public async Task<User> SaveByNameAsync(string userName)
        {
            bool isFirstUser = !(await AnyUsersAsync());

            User newUser = new User
            {
                Name = userName,
                CreatedAt = DateTime.UtcNow,
            };

            newUser = await SaveAsync(newUser);

            if (isFirstUser)
            {
                var adminRole = await _rolesService.GetRoleByNameAsync(Roles.AdminGroupName);
                if (adminRole != null)
                {
                    await AssignRoleToUserAsync(newUser.Id, adminRole.Id);
                }
                else
                {
                    _logger.LogWarning("Admin role not found. Cannot assign it to first user {UserName}.", userName);
                }
            }

            return newUser;
        }

        public async Task<User> UpdateAsync(User user)
        {
            var existingUser = await _context.Users.FindAsync(user.Id);
            if (existingUser == null)
            {
                _logger.LogWarning("User with ID {UserId} not found for update.", user.Id);
                throw new CSVWorkerException($"User with ID {user.Id} not found.");
            }

            existingUser.Name = user.Name;
            existingUser.LastUpdatedAt = DateTime.UtcNow;

            await _context.SaveChangesAsync();
            return existingUser;
        }

        // Delete user from database
        public async Task DeleteAsync(long userId)
        {
            var user = await _context.Users.FindAsync(userId);
            if (user == null)
            {
                _logger.LogWarning("User with ID {UserId} not found for deletion.", userId);
                throw new CSVWorkerException($"User with ID {userId} not found.");
            }
            _context.Users.Remove(user);
            await _context.SaveChangesAsync();
        }

        // Paginated list of users
        public async Task<PagedResult<User>> GetPagedAsync(int? pageNumber, string? query)
        {
            var pageSize = _config.DefaultPageSize;

            if (pageNumber == null || pageNumber < 1) pageNumber = 1;

            IQueryable<User> q = _context.Users;

            if (!string.IsNullOrWhiteSpace(query))
            {
                q = q.Where(r => r.Name.StartsWith(query));
            }

            var total = await q.CountAsync();
            var items = await q
                .Include(u => u.Roles)
                .OrderByDescending(r => r.LastUpdatedAt) // apply ordering after filtering
                .Skip((pageNumber.Value - 1) * pageSize)
                .Take(pageSize)
                .AsNoTracking()
                .ToListAsync();

            return new PagedResult<User>(items, total, pageNumber.Value, pageSize);
        }

        // Get All users with roles
        public async Task<List<User>> GetAllUsersAsync()
        {
            return await _context.Users
                .Include(u => u.Roles)
                .OrderBy(r => r.Name)
                .ToListAsync();
        }

        // Assign a roleId to a UserId
        public async Task AssignRoleToUserAsync(long userId, long roleId)
        {
            var user = await GetUserByIdAsync(userId);
            var role = await _rolesService.GetRoleByIdAsync(roleId);

            if (user == null)
            {
                _logger.LogWarning("User with ID {UserId} not found for role assignment.", userId);
                throw new CSVWorkerException($"User with ID {userId} not found.");
            }

            if (role == null)
            {
                _logger.LogWarning("Role with ID {RoleId} not found for assignment to user.", roleId);
                throw new CSVWorkerException($"Role with ID {roleId} not found.");
            }

            if (!user.Roles.Any(r => r.Id == roleId))
            {
                user.Roles.Add(role);
                await _context.SaveChangesAsync();
            }
        }

        // Remove a roleId from a UserId
        public async Task RemoveRoleFromUserAsync(long userId, long roleId)
        {
            var user = await GetUserByIdAsync(userId);
            var role = await _rolesService.GetRoleByIdAsync(roleId);

            if (user == null)
            {
                _logger.LogWarning("User with ID {UserId} not found for role assignment.", userId);
                throw new CSVWorkerException($"User with ID {userId} not found.");
            }

            if (role == null)
            {
                _logger.LogWarning("Role with ID {RoleId} not found for assignment to user.", roleId);
                throw new CSVWorkerException($"Role with ID {roleId} not found.");
            }

            if (user.Roles.Any(r => r.Id == roleId))
            {
                user.Roles.Remove(role);
                await _context.SaveChangesAsync();
            }
        }

        // Update user roles from a list of roleIds
        // If a roleId is not in the list but user has it, it will be removed from the user
        // if a roleId is in the list but user does not have it, it will be added to the user
        public async Task UpdateUserRolesAsync(long userId, List<long> roleIds)
        {
            var user = await GetUserByIdAsync(userId);
            if (user == null)
            {
                _logger.LogWarning("User with ID {UserId} not found for role update.", userId);
                throw new CSVWorkerException($"User with ID {userId} not found.");
            }

            // Get all roles from the database
            var allRoles = await _rolesService.GetAllRolesAsync();

            // Remove roles that are not in the new list
            foreach (var role in user.Roles.ToList())
            {
                if (!roleIds.Contains(role.Id))
                {
                    user.Roles.Remove(role);
                }
            }

            // Add new roles that the user does not have
            foreach (var roleId in roleIds)
            {
                if (!user.Roles.Any(r => r.Id == roleId))
                {
                    var roleToAdd = await _rolesService.GetRoleByIdAsync(roleId);
                    if (roleToAdd != null)
                    {
                        user.Roles.Add(roleToAdd);
                    }
                }
            }
            await _context.SaveChangesAsync();
        }
    }
}
