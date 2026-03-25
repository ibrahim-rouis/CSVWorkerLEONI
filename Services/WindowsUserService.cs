using CSVWorker.Models.Entities;
using CSVWorker.Services.LDAP;

namespace CSVWorker.Services
{
    public class WindowsUserService
    {
        private readonly UsersService _usersService;
        private readonly LdapService _ldapService;
        private readonly ILogger<WindowsUserService> _logger;
        private readonly RolesService _rolesService;
        public WindowsUserService(UsersService userService, ILogger<WindowsUserService> logger, RolesService rolesService, LdapService ldapService)
        {
            _usersService = userService;
            _logger = logger;
            _rolesService = rolesService;
            _ldapService = ldapService;
        }

        public async Task<User?> GetWindowsUserByName(string username)
        {
            try
            {
                // Check if user exists in database
                if (await _usersService.UserNameExists(username))
                {
                    var user = await _usersService.GetUserByName(username);
                    return user;
                }
                else
                {
                    _logger.LogWarning("Windows user {Username} not found in database.", username);
                    return null;
                }
            }
            catch (Exception ex)
            {
                // Something fatal happened went wrong and need to be thrown
                // This should not happen, but we log it just in case
                _logger.LogError(ex, "Error getting Windows user from HttpContext");
                throw;
            }
        }

        //  Save user and roles assigned to it into database
        public async Task<User?> SaveWindowsUser(string username, List<string> ldapGroups)
        {
            try
            {
                // Check if user exists in database
                // The SaveUserToDbIfNotExist method should have already checked if the user exists in database, but we check it again
                if (await _usersService.UserNameExists(username))
                {
                    _logger.LogError("Windows user {Username} already exists in database, we can't save it twice.", username);
                    throw new Exception($"Windows user {username} already exists in database");
                }

                /* Get user roles and save new ones in database */


                // Roles Ids to assign to user
                List<int> userRolesIds = new List<int>();

                if (ldapGroups != null && ldapGroups.Count > 0)
                {
                    foreach (var rolename in ldapGroups)
                    {
                        // Check if null
                        if (rolename == null)
                        {
                            continue;
                        }

                        // check if role name already exists?
                        if (await _rolesService.RoleNameExists(rolename))
                        {
                            Role? role = await _rolesService.GetRoleByName(rolename);
                            if (role != null)
                            {
                                userRolesIds.Add(role.Id);
                            }
                        }
                        else
                        {
                            // create role in database
                            Role role = await _rolesService.CreateRole(new Role { Name = rolename });
                            userRolesIds.Add(role.Id);
                        }

                    }
                }

                // Get UserPhoto from LDAP
                var userPhotoBytes = _ldapService.GetUserPhoto(username);

                var newUser = new User
                {
                    Name = username,
                    Photo = userPhotoBytes, // We don't have user photo from Windows authentication, so we set it to null
                };

                // Save Windows user in database
                var createdUser = await _usersService.CreateUser(newUser, userRolesIds.ToArray());
                _logger.LogInformation("Windows user {Username} created in database with id={UserId}", username, createdUser.Id);
                return createdUser;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error saving Windows user {Username} in database", username);
                throw;
            }
        }

        // Update user roles
        public async Task<User?> UpdateUserRoles(string username, List<string> ldapGroups)
        {
            try
            {
                // Retrieve the existing user
                var user = await GetWindowsUserByName(username);
                if (user == null)
                {
                    _logger.LogWarning("Cannot update roles for {Username} because the user does not exist in the database.", username);
                    return null;
                }

                List<int> userRolesIds = new List<int>();

                if (ldapGroups != null && ldapGroups.Count > 0)
                {
                    foreach (var rolename in ldapGroups)
                    {
                        if (string.IsNullOrEmpty(rolename))
                        {
                            continue;
                        }

                        // Check if role name already exists
                        if (await _rolesService.RoleNameExists(rolename))
                        {
                            Role? role = await _rolesService.GetRoleByName(rolename);
                            if (role != null)
                            {
                                userRolesIds.Add(role.Id);
                            }
                        }
                        else
                        {
                            // Create role in database
                            Role role = await _rolesService.CreateRole(new Role { Name = rolename });
                            userRolesIds.Add(role.Id);
                        }
                    }
                }

                // Update the user's roles in the database
                await _usersService.UpdateUserRoles(user.Id, user, userRolesIds.ToArray());

                _logger.LogInformation("Successfully updated roles for Windows user {Username}.", username);

                return user;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error updating roles for Windows user {Username}", username);
                return null;
            }
        }
    }
}
