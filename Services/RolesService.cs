using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using CSVWorker.Configuration;
using CSVWorker.Models;
using CSVWorker.Models.Common;
using CSVWorker.Models.Entities;

namespace CSVWorker.Services
{
    public class RolesService
    {
        private readonly CSVWorkerDBContext _context;
        private readonly CSVWorkerConfig _config;
        private readonly ILogger<RolesService> _logger;
        public RolesService(CSVWorkerDBContext context, IOptions<CSVWorkerConfig> CSVWorkerConfig, ILogger<RolesService> logger)
        {
            _context = context;
            _config = CSVWorkerConfig.Value;
            _logger = logger;
        }

        /// <summary>
        /// Retrieves a paginated list of roles, optionally filtered by a search string.
        /// </summary>
        /// <remarks>Results are ordered alphabetically by role name to ensure consistent paging. The page
        /// size is determined by the application's default configuration.</remarks>
        /// <param name="searchString">An optional search string used to filter roles by name. The filter is case-insensitive. If null or empty,
        /// all roles are returned.</param>
        /// <param name="pageIndex">The zero-based index of the page to retrieve. Must be greater than or equal to 0.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a paginated list of roles
        /// matching the search criteria for the specified page.</returns>
        public async Task<PaginatedList<Role>> GetRoles(string searchString, int pageIndex)
        {
            _logger.LogInformation("Getting roles with search string: {SearchString} and page index: {PageIndex}", searchString, pageIndex);

            // NoTracking is recommended to make pages consistent when something changes during viewing it
            var roles = _context.Roles.AsNoTracking();
            // Apply search filer if provided

            // Apply searchString filter (case insensitive)
            if (!string.IsNullOrEmpty(searchString))
            {
                roles = roles.Where(u => u.Name!.ToUpper().Contains(searchString.ToUpper()));
            }

            // Add OrderBy to ensrue consistent ordering of results
            roles = roles.OrderBy(p => p.Name);

            return await PaginatedList<Role>.CreateAsync(roles, pageIndex, _config.DefaultPageSize);
        }

        /// <summary>
        /// Retrieves a role by its unique identifier. Returns null if no role with the specified ID exists.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<Role?> GetRoleById(int id)
        {
            _logger.LogInformation("Getting role by id: {Id}", id);
            return await _context.Roles.FirstOrDefaultAsync(p => p.Id == id);
        }

        public async Task<Role?> GetRoleByName(string name)
        {
            _logger.LogInformation("Getting role by name: {Name}", name);
            return await _context.Roles.FirstOrDefaultAsync(p => p.Name == name);
        }

        /// <summary>
        /// Creates a new role in the data store.
        /// </summary>
        /// <param name="role">The role to create. Must not be null and must have a unique identifier.</param>
        /// <returns>A task that represents the asynchronous operation.</returns>
        /// <exception cref="InvalidOperationException">Thrown if a role with the same identifier already exists.</exception>
        public async Task<Role> CreateRole(Role role)
        {
            if (await RoleExists(role.Id))
            {
                _logger.LogWarning("Role with id {Id} already exists during role creation", role.Id);
                throw new InvalidOperationException($"Role with id {role.Id} already exists.");
            }

            _logger.LogInformation("Creating new role with name: {RoleName}", role.Name);
            _context.Roles.Add(role);
            await _context.SaveChangesAsync();

            return role;
        }

        /// <summary>
        /// Updates the specified role in the data store.
        /// </summary>
        /// <remarks>If a concurrency conflict occurs during the update, the method logs the error and
        /// rethrows the exception unless the role no longer exists.</remarks>
        /// <param name="role">The role entity containing updated information. The role's Id must correspond to an existing role.</param>
        /// <returns>A task that represents the asynchronous update operation.</returns>
        /// <exception cref="KeyNotFoundException">Thrown if a role with the specified Id does not exist.</exception>
        public async Task UpdateRole(Role role)
        {
            try
            {
                if (!(await RoleExists(role.Id)))
                {
                    _logger.LogWarning("Role with id {Id} not found during role update", role.Id);
                    throw new KeyNotFoundException($"Role with id {role.Id} not found.");
                }

                _logger.LogInformation("Updating role with id: {Id} and new name: {RoleName}", role.Id, role.Name);
                _context.Roles.Update(role);
                await _context.SaveChangesAsync();
            }
            catch (DbUpdateConcurrencyException ex)
            {
                _logger.LogError(ex, "Concurrency error while updating role with id={Id}", role.Id);
                if (!await RoleExists(role.Id))
                {
                    _logger.LogWarning("Edit POST action: No role found with id={Id} during update", role.Id);
                }
                else
                {
                    throw;
                }
            }

        }

        /// <summary>
        /// Deletes the specified role from the data store and removes its association from all users.
        /// </summary>
        /// <remarks>If the role is associated with any users, their role associations are updated to
        /// remove the deleted role before the role is removed from the database. Changes are persisted to the data
        /// store upon completion.</remarks>
        /// <param name="role">The role to delete. Must not be null and should include any associated users whose role associations will be
        /// updated.</param>
        /// <returns>A task that represents the asynchronous delete operation.</returns>
        public async Task DeleteRole(Role role)
        {
            _logger.LogInformation("Deleting role with id: {Id} and name: {RoleName}", role.Id, role.Name);

            // Check if any users are assosiated with this role
            if (role.Users != null && role.Users.Any())
            {
                _logger.LogInformation("Removing role associations from {UserCount} users", role.Users.Count);
                // Loop through each user and remove the role association
                foreach (var user in role.Users)
                {
                    user.Roles!.Remove(role);
                }
            }

            // Remove role from database
            _context.Roles.Remove(role);
            await _context.SaveChangesAsync();
        }


        /// <summary>
        /// Determines whether a role with the specified identifier exists in the data store.
        /// </summary>
        /// <param name="id">The unique identifier of the role to check for existence.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains <see langword="true"/> if a role
        /// with the specified identifier exists; otherwise, <see langword="false"/>.</returns>
        public async Task<bool> RoleExists(int id)
        {
            return await _context.Roles.AnyAsync(p => p.Id == id);
        }

        public async Task<bool> RoleNameExists(string name)
        {
            return await _context.Roles.AnyAsync(r => r.Name == name);
        }

        /// <summary>
        /// Retrieves a role by its unique identifier, including the associated users.
        /// </summary>
        /// <remarks>The returned role includes its related users loaded from the database. This method
        /// performs a database query and may incur network or I/O latency.</remarks>
        /// <param name="id">The unique identifier of the role to retrieve.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the role with its associated
        /// users if found; otherwise, null.</returns>
        public async Task<Role?> GetRoleByIdWithUsers(int id)
        {
            _logger.LogInformation("Getting role with users by id: {Id}", id);
            return await _context.Roles.Include(p => p.Users).FirstOrDefaultAsync(p => p.Id == id);
        }

        /// <summary>
        /// Asynchronously retrieves all roles ordered by name.
        /// </summary>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of all roles, ordered
        /// alphabetically by name. The list will be empty if no roles are found.</returns>
        public async Task<List<Role>> GetAllRoles()
        {
            _logger.LogInformation("Getting all roles ordered by name");
            return await _context.Roles.OrderBy(p => p.Name).ToListAsync();
        }

        /// <summary>
        /// Retrieves a list of roles that match the specified role IDs.
        /// </summary>
        /// <param name="ids">A list of role IDs to retrieve. Cannot be null or empty.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a list of roles corresponding to
        /// the specified IDs. The list will be empty if no matching roles are found.</returns>
        public async Task<List<Role>> GetRolesByIds(List<int> ids)
        {
            _logger.LogInformation("Getting roles by ids: {Ids}", string.Join(", ", ids));
            return await _context.Roles.Where(p => ids.Contains(p.Id)).ToListAsync();
        }
    }
}
