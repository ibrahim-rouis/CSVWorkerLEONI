namespace CSVWorker.Security
{
    public static class Roles
    {
        // Write the standard NT (Windows NT) naming convention for Active Directory groups

        // Admin NT group name in AD
        public const string AdminGroupName = @"LEONI\IT_Admin";

        // Manager NT group name in AD (this is for the material compliance team)
        public const string ManagerGroupName = @"LEONI\IT_COMPLIANCE";
    }
}
