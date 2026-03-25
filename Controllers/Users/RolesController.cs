using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using CSVWorker.Models.Entities;
using CSVWorker.Services;

namespace CSVWorker.Controllers.Users
{
    [Authorize(Roles = "Admins")]
    public class RolesController : Controller
    {
        private readonly RolesService _service;
        private readonly ILogger<RolesController> _logger;

        public RolesController(RolesService service, ILogger<RolesController> logger)
        {
            _service = service;
            _logger = logger;
        }

        // GET: Roles
        public async Task<IActionResult> Index(int? pageNumber, string searchString)
        {
            _logger.LogInformation("Index action called by user {User} with pageNumber={PageNumber} and searchString={SearchString}", User.Identity?.Name, pageNumber, searchString);
            // Page index (1 by default)
            int pageIndex = pageNumber ?? 1;

            // negative values reset to default
            if (pageIndex < 1)
            {
                pageIndex = 1;
            }

            var roles = await _service.GetRoles(searchString, pageIndex);

            ViewData["SearchString"] = searchString ?? "";

            return View("~/Views/UsersMgr/Roles/Index.cshtml", roles);
        }

        // GET: Roles/Create
        public IActionResult Create()
        {
            _logger.LogInformation("Create GET action called by user {User}", User.Identity?.Name);
            return View("~/Views/UsersMgr/Roles/Create.cshtml");
        }

        // POST: Roles/Create
        // To protect from overposting attacks, enable the specific properties you want to bind to.
        // For more details, see http://go.microsoft.com/fwlink/?LinkId=317598.
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Id,Name")] Role role)
        {
            _logger.LogInformation("Create POST action called by user {User} with role name={RoleName}", User.Identity?.Name, role.Name);
            if (ModelState.IsValid)
            {
                await _service.CreateRole(role);
                return RedirectToAction(nameof(Index));
            }
            return View("~/Views/UsersMgr/Roles/Create.cshtml", role);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Delete(int id)
        {
            _logger.LogInformation("Delete POST action called by user {User} for role id={Id}", User.Identity?.Name, id);

            var role = await _service.GetRoleByIdWithUsers(id);

            if (role == null)
            {
                _logger.LogWarning("Delete POST action: No role found with id={Id}", id);
                return Json(new { success = false, message = "Role not found." });
            }

            await _service.DeleteRole(role);

            // Optionally, you can return pageNumber and searchString in the response if needed
            return Json(new { success = true });
        }
    }
}
