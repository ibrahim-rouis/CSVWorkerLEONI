using Microsoft.AspNetCore.Mvc;
using CSVWorker.Services;

namespace CSVWorker.Controllers.Users
{
    [Route("Account")]
    public class AccountController : Controller
    {
        private readonly WindowsUserService _windowsUserService;

        public AccountController(WindowsUserService windowsUserService)
        {
            _windowsUserService = windowsUserService;
        }

        [HttpGet("ProfilePicture")]
        public async Task<IActionResult> ProfilePicture()
        {
            var defaultAvatarPath = "~/img/avatars/default.svg";

            if (User.Identity != null && User.Identity.IsAuthenticated && !string.IsNullOrEmpty(User.Identity.Name))
            {
                // Fetch from Database (Super Fast) instead of LDAP
                var user = await _windowsUserService.GetWindowsUserByName(User.Identity.Name);

                if (user?.Photo != null)
                {
                    return File(user.Photo, "image/jpeg");
                }
            }

            // Return a default "avatar" icon if no photo exists
            return LocalRedirect(Url.Content(defaultAvatarPath)!);
        }
    }
}
