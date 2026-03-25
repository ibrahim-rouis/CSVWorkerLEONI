using System.DirectoryServices.Protocols;
using System.Net;
using Microsoft.Extensions.Options;
using CSVWorker.Configuration;

namespace CSVWorker.Services.LDAP
{
    public class LdapService
    {
        private readonly LdapConfig _ldapConfig;
        private readonly ILogger<LdapService> _logger;
        private readonly IWebHostEnvironment _env;

        // Inject IOptions<LdapConfig> here
        public LdapService(IOptions<LdapConfig> ldapOptions, ILogger<LdapService> logger, IWebHostEnvironment env)
        {
            _ldapConfig = ldapOptions.Value;
            _logger = logger;
            _env = env;
        }

        public bool ValidateUserCredentials(string username, string password)
        {
            try
            {
                using var connection = CreateConnection();
                string userDn = BuildUserIdentity(username);

                connection.Bind(new NetworkCredential(userDn, password));
                return true;
            }
            catch (LdapException ex)
            {
                _logger.LogWarning("LDAP Bind failed for user {Username}. Error: {Message}", username, ex.Message);
                return false;
            }
        }

        public bool IsUserInGroup(string username, string groupName)
        {
            try
            {
                using var connection = CreateConnection();

                // 1. Bind with Admin credentials using the strongly-typed config
                var adminCreds = new NetworkCredential(_ldapConfig.AdminDn, _ldapConfig.AdminPassword);
                connection.Bind(adminCreds);

                string userDn = BuildUserIdentity(username);

                // 2. Active Directory and OpenLDAP sometimes use different group classes
                string groupClass = _ldapConfig.GroupClass ?? "group";
                string groupFilter = $"(&(objectClass={groupClass})(cn={groupName})(member={userDn}))";

                var searchRequest = new SearchRequest(
                    $"{_ldapConfig.GroupOu},{_ldapConfig.BaseDn}",
                    groupFilter,
                    SearchScope.Subtree,
                    null);

                var response = (SearchResponse)connection.SendRequest(searchRequest);
                return response.Entries.Count > 0;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error checking LDAP group membership for user {Username} and group {GroupName}", username, groupName);
                return false;
            }
        }

        public List<string>? GetUserGroups(string username)
        {
            try
            {
                var groups = new List<string>();

                using var connection = CreateConnection();
                var adminCreds = new NetworkCredential(_ldapConfig.AdminDn, _ldapConfig.AdminPassword);
                connection.Bind(adminCreds);

                string userDn = BuildUserIdentity(username);

                // Filter: Find groups where the current user is a 'member'
                // For Active Directory: (objectClass=group)
                string groupClass = _ldapConfig.GroupClass ?? "group";
                string filter = $"(&(objectClass={groupClass})(member={userDn}))";

                var searchRequest = new SearchRequest(
                    $"{_ldapConfig.GroupOu},{_ldapConfig.BaseDn}",
                    filter,
                    SearchScope.Subtree,
                    new[] { "cn" } // We only need the Common Name (the group name)
                );

                var response = (SearchResponse)connection.SendRequest(searchRequest);

                foreach (SearchResultEntry entry in response.Entries)
                {
                    var cn = entry.Attributes["cn"][0].ToString();
                    if (!string.IsNullOrEmpty(cn)) groups.Add(cn);
                }
                return groups;
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error fetching groups for user {Username}", username);
                return null;
            }
        }

        public byte[]? GetUserPhoto(string username)
        {
            try
            {
                var photoAttribName = _ldapConfig.PhotoAttribName ?? "thumbnailPhoto";
                using var connection = CreateConnection();
                var adminCreds = new NetworkCredential(_ldapConfig.AdminDn, _ldapConfig.AdminPassword);
                connection.Bind(adminCreds);

                string userDn = BuildUserIdentity(username);
                var searchRequest = new SearchRequest(
                    userDn,
                    "(objectClass=*)",
                    SearchScope.Base,
                    new[] { photoAttribName });

                var response = (SearchResponse)connection.SendRequest(searchRequest);
                var entry = response.Entries[0];

                if (entry.Attributes.Contains(photoAttribName))
                {
                    return (byte[])entry.Attributes[photoAttribName][0];
                }
                return null;
            }
            catch
            {
                return null;
            }
        }

        private LdapConnection CreateConnection()
        {
            // No more int.Parse() needed!
            var connection = new LdapConnection(new LdapDirectoryIdentifier(_ldapConfig.Server, _ldapConfig.Port));

            connection.SessionOptions.ProtocolVersion = 3;
            connection.SessionOptions.ReferralChasing = ReferralChasingOptions.None;
            connection.AuthType = AuthType.Negotiate;

            return connection;
        }

        private string BuildUserIdentity(string username)
        {
            // For Active Directory, we can use the UPN format (e.g., user@leoni.local)
            return $"{username}@{_ldapConfig.DomainName}";
        }
    }
}