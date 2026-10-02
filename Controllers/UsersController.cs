using CSVWorker.Exceptions;
using CSVWorker.Models.Entities;
using CSVWorker.Security;
using CSVWorker.Services;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Rendering;

namespace CSVWorker.Controllers
{
    [Authorize(Policy = Policies.AdminPolicy)]
    public class UsersController : Controller
    {

        private readonly ILogger<UsersController> _logger;
        private readonly UsersService _service;
        private readonly RolesService _rolesService;

        public UsersController(ILogger<UsersController> logger, UsersService service, RolesService rolesService)
        {
            _logger = logger;
            _service = service;
            _rolesService = rolesService;
        }

        public async Task<IActionResult> Index(int? pageNumber, string? query)
        {
            _logger.LogInformation("Users Index page accessed by user {Name} with pageNumber={pageNumber} and query={query}.", User.Identity?.Name, pageNumber, query);

            var users = await _service.GetPagedAsync(pageNumber, query);

            ViewData["query"] = query ?? "";

            return View(users);
        }

        [Authorize(Policy = Policies.AdminOrManagerPolicy)]
        public IActionResult Create()
        {
            return View();
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Create([Bind("Name")] User model)
        {
            _logger.LogInformation("Users Create attempt by user {Name} with Username={Username}.", User.Identity?.Name, model.Name);
            if (!ModelState.IsValid)
            {
                return View(model);
            }

            try
            {
                await _service.SaveAsync(model);
                return RedirectToAction(nameof(Index));
            }
            catch (CSVWorkerException ex)
            {
                _logger.LogError(ex, "Error creating User {Username}", model.Name);
                ModelState.AddModelError(string.Empty, ex.Message);
                return View(model);
            }
        }

        public async Task<IActionResult> Edit(long id)
        {
            _logger.LogInformation("Users Edit page accessed by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var user = await _service.GetUserByIdAsync(id);
            if (user == null)
            {
                return NotFound();
            }
            return View(user);
        }

        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> Edit(long id, [Bind("Id,Name")] User model)
        {
            _logger.LogInformation("Users Edit attempt by user {Name} for record ID={id} with new Username={Username}.", User.Identity?.Name, id, model.Name);

            if (!ModelState.IsValid)
            {
                return View(model);
            }

            if (id != model.Id)
            {
                return BadRequest();
            }

            try
            {
                await _service.UpdateAsync(model);
                return RedirectToAction(nameof(Index));
            }
            catch (CSVWorkerException ex)
            {
                _logger.LogError(ex, "Error updating User ID={id} with new Username={Username}", id, model.Name);
                ModelState.AddModelError(string.Empty, ex.Message);
                return View(model);
            }
        }

        // details
        public async Task<IActionResult> Details(long id)
        {
            _logger.LogInformation("Users Details page accessed by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var user = await _service.GetUserByIdAsync(id);
            if (user == null)
            {
                return NotFound();
            }

            return View(user);
        }

        // delete
        public async Task<IActionResult> Delete(long id)
        {
            _logger.LogInformation("Users Delete page accessed by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var user = await _service.GetUserByIdAsync(id);
            if (user == null)
            {
                return NotFound();
            }

            return View(user);
        }

        [HttpPost, ActionName("Delete")]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> DeleteConfirmed(long id)
        {
            _logger.LogInformation("Users Delete attempt by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var record = await _service.GetUserByIdAsync(id);
            if (record == null)
            {
                return NotFound();
            }

            await _service.DeleteAsync(id);

            _logger.LogInformation("User with ID={id} deleted successfully by user {Name}.", id, User.Identity?.Name);

            return RedirectToAction(nameof(Index));
        }

        // update roles view
        public async Task<IActionResult> UpdateRoles(long id)
        {
            _logger.LogInformation("Users UpdateRoles page accessed by user {Name} for record ID={id}.", User.Identity?.Name, id);
            var user = await _service.GetUserByIdAsync(id);
            if (user == null)
            {
                return NotFound();
            }
            // Get all roles and mark the ones assigned to this user as selected
            var allRoles = await _rolesService.GetAllRolesAsync();
            var userRolesIds = user.Roles?.Select(p => p.Id).ToList() ?? new List<long>();
            ViewBag.RolesList = allRoles.Select(p => new SelectListItem
            {
                Value = p.Id.ToString(),
                Text = p.Name,
                Selected = userRolesIds.Contains(p.Id)
            }).ToList();

            return View(user);
        }

        // update roles post
        [HttpPost]
        [ValidateAntiForgeryToken]
        public async Task<IActionResult> UpdateRoles(long id, List<long> selectedRoles)
        {
            _logger.LogInformation("Users UpdateRoles attempt by user {Name} for record ID={id} with selected roles: {SelectedRoles}.", User.Identity?.Name, id, string.Join(", ", selectedRoles));

            var user = await _service.GetUserByIdAsync(id);
            if (user == null)
            {
                return NotFound();
            }

            try
            {
                await _service.UpdateUserRolesAsync(id, selectedRoles);
                return RedirectToAction(nameof(Index));
            }
            catch (CSVWorkerException ex)
            {
                _logger.LogError(ex, "Error updating roles for User ID={id}", id);
                ModelState.AddModelError(string.Empty, $"Failed to update user roles: {ex.Message}");

                // Get all roles and mark the ones assigned to this user as selected
                var allRoles = await _rolesService.GetAllRolesAsync();
                var userRolesIds = user.Roles?.Select(p => p.Id).ToList() ?? new List<long>();
                ViewBag.RolesList = allRoles.Select(p => new SelectListItem
                {
                    Value = p.Id.ToString(),
                    Text = p.Name,
                    Selected = userRolesIds.Contains(p.Id)
                }).ToList();

                return View(user);
            }
        }
    }
}
