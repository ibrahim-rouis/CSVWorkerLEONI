using CSVWorker.Services.LDAP;
using Microsoft.AspNetCore.Mvc;

namespace CSVWorker.Controllers
{
    public class AccountController : Controller
    {
        private readonly LdapService _service;
        private readonly IWebHostEnvironment _env;

        // Default avatar relative path under wwwroot
        private const string DefaultAvatarRelativePath = "img/avatars/default.svg";

        public AccountController(LdapService ldapService, IWebHostEnvironment env)
        {
            _service = ldapService;
            _env = env;
        }

        /// <summary>
        /// Retrieves the authenticated user's profile picture or redirects to a default avatar if unavailable.
        /// </summary>
        /// <returns>An IActionResult containing the user's profile picture as a JPEG image, or a redirect to the default avatar.</returns>
        public IActionResult ProfilePicture()
        {
            Response.Headers["Cache-Control"] = "public, max-age=86400, immutable"; // cache for 1 day

            var userName = User.Identity!.Name!;
            var photoBytes = _service.GetUserPhoto(userName);

            if (photoBytes != null && photoBytes.Length > 0)
            {
                return File(photoBytes, "image/jpeg");
            }


            return LocalRedirect(Url.Content(DefaultAvatarRelativePath)!);
        }
    }
}
