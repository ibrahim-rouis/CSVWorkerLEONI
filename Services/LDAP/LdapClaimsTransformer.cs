using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Caching.Memory;
using System.Security.Claims;

namespace CSVWorker.Services.LDAP
{
    public class LdapClaimsTransformer : IClaimsTransformation
    {
        private readonly WindowsUserService _windowsUserService;
        private readonly LdapService _ldapService;
        private readonly IMemoryCache _cache;
        private readonly IWebHostEnvironment _env;

        public LdapClaimsTransformer(
            WindowsUserService windowsUserService,
            IMemoryCache cache,
            LdapService ldapService,
             IWebHostEnvironment env
        )
        {
            _windowsUserService = windowsUserService;
            _cache = cache;
            _ldapService = ldapService;
            _env = env;
        }

        public async Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal)
        {
            var clone = principal.Clone();
            var newIdentity = (ClaimsIdentity)clone.Identity!;

            if (!newIdentity.IsAuthenticated || string.IsNullOrEmpty(newIdentity.Name))
                return principal;

            string username = newIdentity.Name; // Typically looks like "DOMAIN\username"

            // Strip the domain prefix if your database just stores "username"
            //if (username.Contains("\\"))
            //{
            //    username = username.Split('\\')[1];
            //}

            string cacheKey = $"UserRoles_{username}";

            // Check cache to avoid hitting the database on every HTTP request
            if (!_cache.TryGetValue(cacheKey, out List<string>? cachedRoleNames))
            {
                // 1. Try to get user from DB
                var dbUser = await _windowsUserService.GetWindowsUserByName(username);
                var adGroups = _ldapService.GetUserGroups(username);

                // In development set any connected user as admin to avoid LDAP issues, in production rely on LDAP groups for role assignment
                if (_env.IsDevelopment() && adGroups == null)
                {
                    adGroups = new List<string> { "Admins" };
                }

                // 2. If user doesn't exist in DB, you might want to create them here!
                if (dbUser == null)
                {
                    // create user with roles based on AD groups
                    dbUser = await _windowsUserService.SaveWindowsUser(username, adGroups ?? new List<string>());

                    // If user creation in database fails, don't authenticate, user has to refresh
                    if (dbUser == null) return principal;
                }

                // _ldapService.GetUserGroups returns null in case of error, so don't update groups if null since it will remove all user groups
                if (adGroups != null)
                {
                    await _windowsUserService.UpdateUserRoles(username, adGroups);

                    cachedRoleNames = adGroups.Where(r => !string.IsNullOrEmpty(r)).ToList();
                }
                else
                {
                    // If we can't get groups from LDAP, fallback to using whatever roles we have in the database for this user (if any)
                    cachedRoleNames = dbUser.Roles?
                        .Where(r => r != null && r.Name != null)
                        .Select(r => r.Name!)
                        .ToList() ?? new List<string>();
                }

                // Store in memory cache for 15 minutes
                _cache.Set(cacheKey, cachedRoleNames, TimeSpan.FromMinutes(15));
            }

            // 3. Inject Database roles using a NEW Identity (Works in both Dev & Prod)
            if (cachedRoleNames != null && cachedRoleNames.Count > 0)
            {
                // Create explicitly with ClaimTypes.Role to override WindowsIdentity default behavior
                var appRoleIdentity = new ClaimsIdentity("ApplicationRoles", ClaimTypes.Name, ClaimTypes.Role);

                foreach (var roleName in cachedRoleNames)
                {
                    appRoleIdentity.AddClaim(new Claim(ClaimTypes.Role, roleName));
                }

                // Add the secondary identity containing the roles to the principal
                clone.AddIdentity(appRoleIdentity);
            }

            return clone;
        }
    }
}

