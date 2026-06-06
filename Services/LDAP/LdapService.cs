using CSVWorker.Configuration;
using Microsoft.Extensions.Options;
using System.DirectoryServices.Protocols;
using System.Net;

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

        private string? GetUserDistinguishedName(LdapConnection connection, string username)
        {
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

                var adminCreds = new NetworkCredential(_ldapConfig.AdminDn, _ldapConfig.AdminPassword);
                connection.Bind(adminCreds);

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
                    var cn = entry.Attributes["cn"]?[0]?.ToString();
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

                if (connection.AuthType == AuthType.Basic && !_ldapConfig.UseSsl)
                {
                    _logger.LogWarning("Using Basic auth without SSL. Credentials will be sent in cleartext.");
                }

                var adminCreds = new NetworkCredential(_ldapConfig.AdminDn, _ldapConfig.AdminPassword);
                connection.Bind(adminCreds);

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
            var connection = new LdapConnection(new LdapDirectoryIdentifier(_ldapConfig.Server, _ldapConfig.Port));

            connection.SessionOptions.ProtocolVersion = 3;
            connection.SessionOptions.ReferralChasing = ReferralChasingOptions.None;

            // Use SSL for LDAPS
            if (_ldapConfig.UseSsl)
            {
                connection.SessionOptions.SecureSocketLayer = true;
            }

            // Map configured AuthType to AuthType enum
            connection.AuthType = _ldapConfig.AuthType.ToLowerInvariant() switch
            {
                "negotiate" => AuthType.Negotiate,
                "basic" => AuthType.Basic,
                "ntlm" => AuthType.Ntlm,
                "digest" => AuthType.Digest,
                "external" => AuthType.External,
                _ => AuthType.Negotiate,
            };

            // Just in case you changed auth type to Basic without SSL, log an error and abort to prevent credential leakage
            // By default auth type is negotiate in appsettings.json
            if (_env.IsProduction() && connection.AuthType == AuthType.Basic && !_ldapConfig.UseSsl)
            {
                _logger.LogError("Using Basic auth without SSL. Credentials will be sent in cleartext. Aborting operation.");
                throw new InvalidOperationException("Basic authentication without SSL is not allowed due to security risks.");
            }

            return connection;
        }
    }
}