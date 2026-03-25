using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Options;
using CSVWorker.Configuration;
using CSVWorker.Models;
using CSVWorker.Models.Common;
using CSVWorker.Models.Entities;

namespace CSVWorker.Services
{
    public class UsersService
    {
        private readonly CSVWorkerDBContext _context;
        private readonly CSVWorkerConfig _config;
        private readonly ILogger<UsersService> _logger;
        private readonly RolesService _rolesService;


        public UsersService(CSVWorkerDBContext context, IOptions<CSVWorkerConfig> CSVWorkerConfig, ILogger<UsersService> logger, RolesService rolesService)
        {
            _context = context;
            _config = CSVWorkerConfig.Value;
            _logger = logger;
            _rolesService = rolesService;
        }

        /// <summary>
        /// Retrieves a paginated list of users filtered by search criteria and role, if specified.
        /// </summary>
        /// <remarks>Results are ordered by user ID to ensure consistent paging. The returned list does
        /// not track changes to entities, which helps maintain consistency when viewing pages as data
        /// changes.</remarks>
        /// <param name="searchString">The search term used to filter users by name. If null or empty, no name filtering is applied.</param>
        /// <param name="pageIndex">The zero-based index of the page to retrieve. Must be greater than or equal to 0.</param>
        /// <param name="roleIdFilter">The role identifier used to filter users by role. If null, no role filtering is applied.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains a paginated list of users
        /// matching the specified filters.</returns>
        public async Task<PaginatedList<User>> GetUsers(string searchString, int pageIndex, int? roleIdFilter)
        {
            _logger.LogInformation("Getting users with searchString: {searchString}, pageIndex: {pageIndex}, roleIdFilter: {roleIdFilter}", searchString, pageIndex, roleIdFilter);

            // NoTracking is recommended to make pages consistent when something changes during viewing it
            var users = _context.Users.Include(u => u.Roles).AsNoTracking();

            // Apply search filer if provided
            if (!string.IsNullOrEmpty(searchString))
            {
                users = users.Where(u => u.Name!.ToUpper().Contains(searchString.ToUpper()));
            }
            if (roleIdFilter.HasValue)
            {
                users = users.Where(u => u.Roles!.Any(p => p.Id == roleIdFilter));

            }

            // Add OrderBy to ensure consistent ordering of results
            users = users.OrderBy(u => u.Id);

            return await PaginatedList<User>.CreateAsync(users, pageIndex, _config.DefaultPageSize);
        }

        /// <summary>
        /// Retrieves a user by their unique identifier, including associated roles.
        /// </summary>
        /// <remarks>The returned user object includes related role information. This method performs a
        /// database query and may incur network or I/O latency.</remarks>
        /// <param name="id">The unique identifier of the user to retrieve. Must be a positive integer.</param>
        /// <returns>A task that represents the asynchronous operation. The task result contains the user with the specified
        /// identifier, including their roles, or <see langword="null"/> if no user is found.</returns>
        public async Task<User?> GetUserById(int id)
        {
            _logger.LogInformation("Getting user by id: {id}", id);
            return await _context.Users.Include(u => u.Roles).FirstOrDefaultAsync(u => u.Id == id);
        }

        /// <summary>
        /// Creates a new user and assigns the specified roles.
        /// </summary>
        /// <remarks>The method logs the creation of the user and persists the user and assigned roles to
        /// the database. Ensure that the user does not already exist to avoid duplicate entries. This method is not
        /// thread-safe; concurrent calls may result in race conditions.</remarks>
        /// <param name="user">The user entity to be created. Cannot be null. The user's properties, including name and other relevant
        /// details, must be set prior to calling this method.</param>
        /// <param name="selectedRoles">An array of role identifiers to assign to the user. If empty, no roles will be assigned. Each identifier
        /// must correspond to a valid role.</param>
        /// <returns>A task that represents the asynchronous operation. The task completes when the user has been added to the
        /// database.</returns>
        public async Task<User> CreateUser(User user, int[] selectedRoles)
        {
            if (await UserExists(user.Id))
            {
                _logger.LogWarning("User with id {Id} already exists. Creation aborted.", user.Id);
                throw new InvalidOperationException($"User with id {user.Id} already exists.");
            }

            // Add selected roles to the user
            if (selectedRoles.Length > 0)
            {
                user.Roles = await _rolesService.GetRolesByIds([.. selectedRoles]);
            }
            _logger.LogInformation("Creating user with name: {name}", user.Name);
            _context.Users.Add(user);
            await _context.SaveChangesAsync();

            return user;
        }

        /// <summary>
        /// Updates the specified user record in the database asynchronously.
        /// </summary>
        /// <remarks>If the specified user does not exist in the database, no changes will be made. This
        /// method logs the update operation for auditing purposes.</remarks>
        /// <param name="user">The user entity to update. Must not be null. The user's Id property must correspond to an existing user in
        /// the database.</param>
        /// <returns>A task that represents the asynchronous update operation.</returns>
        public async Task UpdateUser(User user)
        {
            _logger.LogInformation("Updating user with id: {id}", user.Id);
            _context.Users.Update(user);
            await _context.SaveChangesAsync();
        }


        /// <summary>
        /// Deletes the user with the specified ID from the database. If the user does not exist, it simply does nothing.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task DeleteUser(int id)
        {
            _logger.LogInformation("Deleting user with id: {id}", id);
            var user = await this.GetUserById(id);
            if (user != null)
            {
                _context.Users.Remove(user);
                await _context.SaveChangesAsync();
            }
        }

        /// <summary>
        /// Updates the user's name and roles based on the provided new user data and selected role IDs.    
        /// </summary>
        /// <param name="id"></param>
        /// <param name="newUserData"></param>
        /// <param name="selectedRoles"></param>
        /// <returns></returns>
        /// <exception cref="KeyNotFoundException"></exception>
        public async Task UpdateUserRoles(int id, User newUserData, int[] selectedRoles)
        {
            try
            {

                // Get the existing user with their roles from the database
                var userToUpdate = await GetUserById(id);

                if (userToUpdate == null)
                {
                    _logger.LogWarning("User with id {Id} not found during role update", id);
                    throw new KeyNotFoundException($"User with id {id} not found.");
                }

                // Update the user's name
                userToUpdate.Name = newUserData.Name;

                // Update roles
                if (userToUpdate.Roles != null && selectedRoles.Length > 0 && userToUpdate.Roles.Count > 0)
                {
                    var userRoles = userToUpdate.Roles!;
                    var removedRoles = userRoles.Where(r => !selectedRoles.Contains(r.Id)).ToList();
                    var newRolesIds = selectedRoles.Where(r => !userRoles.Any(ur => ur.Id == r)).ToList();

                    if (removedRoles != null && removedRoles.Count > 0)
                    {
                        userToUpdate.Roles.RemoveAll(r => removedRoles.Any(rr => rr.Id == r.Id));
                    }

                    // Add selected roles
                    if (newRolesIds != null && newRolesIds.Count > 0)
                    {
                        var rolesToAdd = await _rolesService.GetRolesByIds([.. newRolesIds]);

                        userToUpdate.Roles.AddRange(rolesToAdd);
                    }

                }
                else if (userToUpdate.Roles != null && selectedRoles.Length > 0 && userToUpdate.Roles.Count == 0)
                {
                    var rolesToAdd = await _rolesService.GetRolesByIds([.. selectedRoles]);

                    userToUpdate.Roles.AddRange(rolesToAdd);
                }
                else if (userToUpdate.Roles != null && selectedRoles.Length == 0)
                {
                    userToUpdate.Roles.Clear();
                }

                await UpdateUser(userToUpdate);
            }
            catch (DbUpdateConcurrencyException ex)
            {
                _logger.LogError(ex, "Concurrency error while updating user with id {Id}", id);
                if (!await UserExists(id))
                {
                    _logger.LogWarning("User with id {Id} no longer exists during update", id);
                }
                else
                {
                    throw;
                }
            }
        }

        /// <summary>
        /// Checks if a user with the specified ID exists in the database.
        /// </summary>
        /// <param name="id"></param>
        /// <returns></returns>
        public async Task<bool> UserExists(int id)
        {
            _logger.LogInformation("Checking if user exists with id: {id}", id);
            return await _context.Users.AnyAsync(e => e.Id == id);
        }

        public async Task<bool> UserNameExists(string name)
        {
            _logger.LogInformation("Checking if user exists with name: {name}", name);
            return await _context.Users.AnyAsync(e => e.Name == name);
        }

        public async Task<User?> GetUserByName(string name)
        {
            _logger.LogInformation("Getting user by name {Name}", name);
            return await _context.Users.Include(u => u.Roles).FirstOrDefaultAsync(u => u.Name == name);
        }
    }
}
