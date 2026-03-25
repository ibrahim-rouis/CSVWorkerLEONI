namespace CSVWorker.Configuration
{
    public class LdapConfig
    {
        public string Server { get; set; } = string.Empty;
        public int Port { get; set; } = 389;
        public string BaseDn { get; set; } = string.Empty;
        public string UserOu { get; set; } = string.Empty;
        public string GroupOu { get; set; } = string.Empty;
        public string AdminDn { get; set; } = string.Empty;
        public string AdminPassword { get; set; } = string.Empty;
        public string DomainName { get; set; } = string.Empty;
        public string PhotoAttribName { get; set; } = string.Empty;
        public string GroupClass { get; set; } = string.Empty;
    }
}
