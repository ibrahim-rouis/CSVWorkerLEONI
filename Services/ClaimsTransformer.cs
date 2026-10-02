using Microsoft.AspNetCore.Authentication;
using Microsoft.Extensions.Caching.Memory;
using System.Security.Claims;

namespace CSVWorker.Services
{
    public class ClaimsTransformer : IClaimsTransformation
    {
        private readonly IMemoryCache _cache;
        private readonly IWebHostEnvironment _env;
        private readonly UsersService _service;
        private readonly ILogger<ClaimsTransformer> _logger;

        public ClaimsTransformer(
            IMemoryCache cache,
            IWebHostEnvironment env,
            UsersService usersService,
            ILogger<ClaimsTransformer> logger
        )
        {
            _cache = cache;
            _env = env;
            _service = usersService;
            _logger = logger;
        }

        public async Task<ClaimsPrincipal> TransformAsync(ClaimsPrincipal principal)
        {
            var clone = principal.Clone();
            var newIdentity = (ClaimsIdentity)clone.Identity!;

            if (!newIdentity.IsAuthenticated || string.IsNullOrEmpty(newIdentity.Name))
                return principal;

            string username = newIdentity.Name; // Typically looks like "DOMAIN\username"

            string cacheKey = $"UserRoles_{username}";

            // Check cache to avoid hitting the database on every HTTP request
            if (!_cache.TryGetValue(cacheKey, out List<string>? cachedRoleNames))
            {
                // 1. Try to get user from DB
                var user = await _service.GetUserByNameAsync(username);

                // 2. If user doesn't exist in DB, crerate a new one
                if (user == null)
                {
                    // create new user in database
                    try
                    {
                        user = await _service.SaveByNameAsync(username);
                    }
                    catch (Exception ex)
                    {
                        // If user creation in database fails, don't authenticate, user has to refresh
                        _logger.LogError(ex, "Failed to create user in database for username: {Username}", username);
                        return principal;
                    }
                }
                cachedRoleNames = user.Roles?
                    .Where(r => r != null && r.Name != null)
                    .Select(r => r.Name)
                    .ToList() ?? [];

                // Store in memory cache for 1 minute
                _cache.Set(cacheKey, cachedRoleNames, TimeSpan.FromMinutes(1));
            }

            // 3. Inject Database roles using a NEW Identity
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

