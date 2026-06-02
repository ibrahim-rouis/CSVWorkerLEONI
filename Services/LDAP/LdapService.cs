using System.DirectoryServices.Protocols;
using System.Net;
using CSVWorker.Configuration;
using Microsoft.Extensions.Options;

namespace CSVWorker.Services.LDAP
{
    public class LdapService
    {
        private readonly LdapConfig _ldapConfig;
        private readonly ILogger<LdapService> _logger;

        // Inject IOptions<LdapConfig> here
        public LdapService(IOptions<LdapConfig> ldapOptions, ILogger<LdapService> logger, IWebHostEnvironment env)
        {
            _ldapConfig = ldapOptions.Value;
            _logger = logger;
        }

        /// <summary>
        /// Retrieves the names of groups that the specified user is a member of from the LDAP directory.
        /// </summary>
        /// <param name="username">The username whose group memberships are to be retrieved.</param>
        /// <returns>A list of group names the user belongs to, or null if an error occurs.</returns>
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

        /// <summary>
        /// Retrieves the photo of a specified user from the directory service.
        /// </summary>
        /// <param name="username">The username of the user whose photo is to be retrieved.</param>
        /// <returns>A byte array containing the user's photo, or null if the photo is not found or an error occurs.</returns>
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

        /// <summary>
        /// Creates and configures an LDAP connection using the specified server and port from the configuration.
        /// </summary>
        /// <returns>A configured LdapConnection instance.</returns>
        private LdapConnection CreateConnection()
        {
            var connection = new LdapConnection(new LdapDirectoryIdentifier(_ldapConfig.Server, _ldapConfig.Port));

            connection.SessionOptions.ProtocolVersion = 3;
            connection.SessionOptions.ReferralChasing = ReferralChasingOptions.None;
            connection.AuthType = AuthType.Negotiate;

            return connection;
        }
        
        /// <summary>
        /// Constructs a user principal name (UPN) by combining the specified username with the configured domain name.
        /// </summary>
        /// <param name="username">The username to include in the UPN.</param>
        /// <returns>A string representing the user identity in UPN format.</returns>
        private string BuildUserIdentity(string username)
        {
            // For Active Directory, we can use the UPN format (e.g., user@leoni.local)
            return $"{username}@{_ldapConfig.DomainName}";
        }
    }
}