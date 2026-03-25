using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;
using CSVWorker.Models.Entities;
using CSVWorker.Services;

namespace CSVWorker.Controllers.Users
{
    [Authorize(Roles = "Admins")]
    public class UsersController : Controller
    {
        private readonly ILogger<UsersController> _logger;
        private readonly UsersService _service;
        private readonly RolesService _rolesService;
        public UsersController(UsersService service, RolesService rolesService, ILogger<UsersController> logger)
        {
            _rolesService = rolesService;
            _service = service;
            _logger = logger;
        }

        // GET: Users
        public async Task<IActionResult> Index(int? pageNumber, string searchString, int? roleId)
        {
            _logger.LogInformation("Index action called by user {User} with pageNumber={PageNumber}, searchString={SearchString}, roleId={RoleId}", User.Identity?.Name, pageNumber, searchString, roleId);

            // Redirect to ensure query parameters are always visible in URL
            if (pageNumber == null)
            {
                return RedirectToAction(nameof(Index), new { pageNumber = 1, searchString = searchString ?? "", roleId });
            }

            // Page index
            int pageIndex = pageNumber.Value;

            // Handle negative page index and reset to default
            if (pageIndex < 1)
            {
                return RedirectToAction(nameof(Index), new { pageNumber = 1, searchString = searchString ?? "", roleId });
            }

            // Get all roles for roles filter dropdown
            var roles = await _rolesService.GetAllRoles();
            ViewBag.RolesList = new SelectList(roles, "Id", "Name");

            var users = await _service.GetUsers(searchString ?? "", pageIndex, roleId);

            ViewData["SearchString"] = searchString ?? "";
            ViewData["RoleId"] = roleId;
            return View("~/Views/UsersMgr/Users/Index.cshtml", users);
        }

        // GET: Users/Edit/5
        public async Task<IActionResult> Edit(int? id)
        {
            _logger.LogInformation("Edit GET action called by user {User} with id={Id}", User.Identity?.Name, id);
            if (id == null)
            {
                _logger.LogWarning("Users/Edit called without an id");
                return NotFound();
            }

            var user = await _service.GetUserById(id.Value);

            if (user == null)
            {
                _logger.LogWarning("User with id {Id} not found for editing", id);
                return NotFound();
            }

            // Get all roles and mark the ones assigned to this user as selected
            var allRoles = await _rolesService.GetAllRoles();
            var userRolesIds = user.Roles?.Select(p => p.Id).ToList() ?? new List<int>();
            ViewBag.RolesList = allRoles.Select(p => new SelectListItem
            {
                Value = p.Id.ToString(),
                Text = p.Name,
                Selected = userRolesIds.Contains(p.Id)
            }).ToList();
            return View("~/Views/UsersMgr/Users/Edit.cshtml", user);
        }

        // POST: Users/Edit/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(int id, [Bind("Id,Name")] User user, int[] selectedRoles)
        {
            _logger.LogInformation("Edit POST action called by user {User} with id={Id} and selectedRoles={SelectedRoles}", User.Identity?.Name, id, string.Join(",", selectedRoles));

            if (id != user.Id)
            {
                _logger.LogWarning("User ID in URL does not match user ID in form data");
                return NotFound();
            }

            if (ModelState.IsValid)
            {
                // Update user roles based on selected roles
                await _service.UpdateUserRoles(id, user, selectedRoles);
                return RedirectToAction(nameof(Index), new { pageNumber = 1, searchString = "", roleId = (int?)null });
            }

            // If we got here, something failed, redisplay form
            var allRoles = await _rolesService.GetAllRoles();
            ViewBag.RolesList = allRoles.Select(p => new SelectListItem
            {
                Value = p.Id.ToString(),
                Text = p.Name,
                Selected = selectedRoles?.Contains(p.Id) ?? false
            }).ToList();

            return View("~/Views/UsersMgr/Users/Edit.cshtml", user);
        }

        // POST: Users/Delete/5
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Delete(int id)
        {
            _logger.LogInformation("Delete POST action called by user {User} with id={Id}", User.Identity?.Name, id);
            var user = await _service.GetUserById(id);
            if (user != null)
            {
                await _service.DeleteUser(id);
            }
            else
            {
                _logger.LogWarning("User with id {Id} not found during deletion", id);
                return Json(new { success = false, message = "Role not found." });
            }

            return Json(new { success = true });
        }
    }
}
