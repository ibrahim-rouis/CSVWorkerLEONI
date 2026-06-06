using CSVWorker.Configuration;
using Microsoft.Extensions.Options;
using System.DirectoryServices.Protocols;

namespace CSVWorker.Services.LDAP
{
    public class LdapService
    {
        private readonly LdapConfig _ldapConfig;
        private readonly ILogger<LdapService> _logger;

        // Inject IOptions<LdapConfig> here
        public LdapService(IOptions<LdapConfig> ldapOptions, ILogger<LdapService> logger)
        {
            _ldapConfig = ldapOptions.Value;
            _logger = logger;
        }

        // Used to escape special characters in LDAP filters to prevent injection attacks
        private static string EscapeLdapFilter(string input)
        {
            return input.Replace("\\", "\\5c").Replace("*", "\\2a").Replace("(", "\\28").Replace(")", "\\29").Replace("\0", "\\00");
        }

        private string? GetUserDistinguishedName(LdapConnection connection, string username)
        {
            username = EscapeLdapFilter(username);

            // Try to find user by userPrincipalName (UPN) or sAMAccountName
            string upn = $"{username}@{_ldapConfig.DomainName}";
            string filter = $"(|(userPrincipalName={upn})(sAMAccountName={username}))";
            var req = new SearchRequest(
                _ldapConfig.BaseDn,
                filter,
                SearchScope.Subtree,
                new[] { "distinguishedName" });
            var res = (SearchResponse)connection.SendRequest(req);
            if (res.Entries.Count == 0) return null;
            var dnAttr = res.Entries[0].Attributes["distinguishedName"];
            return dnAttr?.Count > 0 ? dnAttr[0].ToString() : null;
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
                connection.Bind();

                var userDn = GetUserDistinguishedName(connection, username);
                if (string.IsNullOrEmpty(userDn))
                {
                    _logger.LogWarning("User DN not found for {Username}", username);
                    return groups;
                }

                string groupClass = _ldapConfig.GroupClass ?? "group";
                string filter = $"(&(objectClass={groupClass})(member={userDn}))";

                var searchRequest = new SearchRequest(
                    $"{_ldapConfig.GroupOu},{_ldapConfig.BaseDn}",
                    filter,
                    SearchScope.Subtree,
                    new[] { "cn" }); // We only need the Common Name (the group name)

                var response = (SearchResponse)connection.SendRequest(searchRequest);
                foreach (SearchResultEntry entry in response.Entries)
                {
                    // Safely check if the entry contains the "cn" attribute
                    if (entry.Attributes.Contains("cn"))
                    {
                        var cnAttribute = entry.Attributes["cn"];
                        // Check if the attribute actually has values
                        if (cnAttribute.Count > 0)
                        {
                            var cn = cnAttribute[0]?.ToString();
                            if (!string.IsNullOrEmpty(cn))
                            {
                                groups.Add(cn);
                            }
                        }
                    }
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
                connection.Bind();

                var userDn = GetUserDistinguishedName(connection, username);
                if (string.IsNullOrEmpty(userDn)) return null;

                var searchRequest = new SearchRequest(
                    userDn,
                    "(objectClass=*)",
                    SearchScope.Base,
                    new[] { photoAttribName });

                var response = (SearchResponse)connection.SendRequest(searchRequest);
                if (response.Entries.Count == 0) return null;
                var entry = response.Entries[0];

                if (entry.Attributes.Contains(photoAttribName))
                {
                    return (byte[])entry.Attributes[photoAttribName][0];
                }
                return null;
            }
            catch (Exception ex)
            {
                _logger.LogDebug(ex, "GetUserPhoto failed for {Username}", username);
                return null;
            }
        }

        /// <summary>
        /// Creates and configures an LDAP connection using the specified server and port from the configuration.
        /// </summary>
        /// <returns>A configured LdapConnection instance.</returns>
        private LdapConnection CreateConnection()
        {
            var connection = new LdapConnection(new LdapDirectoryIdentifier(_ldapConfig.DomainName, _ldapConfig.Port));

            connection.SessionOptions.ProtocolVersion = 3;
            connection.SessionOptions.ReferralChasing = ReferralChasingOptions.None;
            connection.AuthType = AuthType.Negotiate;

            return connection;
        }
    }
}