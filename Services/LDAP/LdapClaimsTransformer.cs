using CSVWorker.Security;
using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Caching.Memory;
using System.Security.Claims;

namespace CSVWorker.Services.LDAP
{
    public class LdapClaimsTransformer : IClaimsTransformation
    {
        private readonly LdapService _ldapService;
        private readonly IMemoryCache _cache;
        private readonly IWebHostEnvironment _env;
        private static readonly TimeSpan CacheDuration = TimeSpan.FromMinutes(1);

        public LdapClaimsTransformer(
            IMemoryCache cache,
            LdapService ldapService,
             IWebHostEnvironment env
        )
        {
            _cache = cache;
            _ldapService = ldapService;
            _env = env;
        }

        public Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal)
        {
            var clone = principal.Clone();
            var newIdentity = (ClaimsIdentity)clone.Identity!;

            if (!newIdentity.IsAuthenticated || string.IsNullOrEmpty(newIdentity.Name))
                return Task.FromResult(principal);

            string username = newIdentity.Name;

            string cacheKey = $"UserRoles_{username}";

            // Check cache to avoid hitting the database on every HTTP request
            if (!_cache.TryGetValue(cacheKey, out List<string>? cachedRoleNames))
            {
                if (cachedRoleNames == null)
                {
                    cachedRoleNames = new List<string>();
                }

                // Get user groups from LDAP
                var adGroups = _ldapService.GetUserGroups(username);

                // Updated cached roles list based on LDAP groups
                if (adGroups != null)
                {
                    cachedRoleNames.AddRange(adGroups.Where(r => !string.IsNullOrEmpty(r)));
                }
                // In development mode only set any connected user as admin if LDAP is not configured or returns no groups.
                else if (_env.IsDevelopment() && adGroups == null)
                {
                    cachedRoleNames.Add(Roles.Admin);
                }

                // Store in memory cache
                _cache.Set(cacheKey, cachedRoleNames, CacheDuration);
            }

            // Inject LDAP roles using a new Identity
            if (cachedRoleNames != null && cachedRoleNames.Count > 0)
            {
                // Create explicitly with ClaimTypes.Role
                var appRoleIdentity = new ClaimsIdentity("ApplicationRoles", ClaimTypes.Name, ClaimTypes.Role);

                foreach (var roleName in cachedRoleNames)
                {
                    appRoleIdentity.AddClaim(new Claim(ClaimTypes.Role, roleName));
                }

                // Add the secondary identity containing the roles to the principal
                clone.AddIdentity(appRoleIdentity);
            }

            return Task.FromResult(clone);
        }
    }
}

