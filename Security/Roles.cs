namespace CSVWorker.Security
{
    public static class Roles
    {
        public const string Admin = "Admin";
        public const string MaterialCompliance = "MaterialCompliance";
        public const string AdminOrMaterialCompliance = $"{Admin},{MaterialCompliance}";
    }
}
