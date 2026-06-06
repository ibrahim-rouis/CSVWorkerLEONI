namespace CSVWorker.Configuration
{
    public class LdapConfig
    {
        public int Port { get; set; } = 389;
        public string BaseDn { get; set; } = string.Empty;
        public string UserOu { get; set; } = string.Empty;
        public string GroupOu { get; set; } = string.Empty;
        public string DomainName { get; set; } = string.Empty;
        public string PhotoAttribName { get; set; } = string.Empty;
        public string GroupClass { get; set; } = "group";
    }
}
