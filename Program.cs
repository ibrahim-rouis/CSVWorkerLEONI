using CSVWorker.Configuration;
using CSVWorker.Data;
using CSVWorker.Models;
using CSVWorker.Security;
using CSVWorker.Services;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Negotiate;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.EntityFrameworkCore;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

// Bump Kestrel maximum request body size (e.g. 100 MB)
builder.Services.Configure<KestrelServerOptions>(options =>
{
    options.Limits.MaxRequestBodySize = 104857600; // 100 MB
});

// Bump ASP.NET Core Form limits to handle large multipart uploads (100 MB)
builder.Services.Configure<FormOptions>(options =>
{
    options.MultipartBodyLengthLimit = 104857600;
});

// Use Serilog for all logging
builder.Services.AddSerilog((services, lc) => lc
.ReadFrom.Configuration(builder.Configuration)
.ReadFrom.Services(services));

// Register the DbContext with the connection string and SQLite provider
var connectionString = builder.Configuration.GetConnectionString("SqliteConnection");
builder.Services.AddDbContext<CSVWorkerDBContext>(options =>
    options.UseSqlite(connectionString));

builder.Services.AddSingleton<IHttpContextAccessor, HttpContextAccessor>();

// Add services to the container.
builder.Services.AddControllersWithViews();

/* ************* Services ***************** */

// Add memory caching and claims transformation services
builder.Services.AddMemoryCache();
builder.Services.AddTransient<IClaimsTransformation, ClaimsTransformer>();


// IMDS Services
builder.Services.AddScoped<IMDSMacrosService>();
builder.Services.AddScoped<IMDSDatabaseService>();
builder.Services.AddScoped<IMDSPorscheDatabaseService>();
builder.Services.AddScoped<UsersService>();
builder.Services.AddScoped<RolesService>();

// Logs service
builder.Services.AddScoped<LogViewerService>();


/* ************* Custom configurations binding setup ***************** */

// Bind CsvWorkerConfig from appsettings.json to the CsvWorkerConfig class and make it available for injection
builder.Services.Configure<CSVWorkerConfig>(builder.Configuration.GetSection("CsvWorkerConfig"));

/* ************* Authentication Setup  ***************** */

builder.Services.AddAuthentication(NegotiateDefaults.AuthenticationScheme).AddNegotiate();


builder.Services.AddAuthorization(options =>
{
    options.FallbackPolicy = options.DefaultPolicy;
    options.AddPolicy(Policies.AdminPolicy, p => p.RequireRole(Roles.AdminGroupName));
    options.AddPolicy(Policies.ManagerPolicy, p => p.RequireRole(Roles.ManagerGroupName));
    options.AddPolicy(Policies.AdminOrManagerPolicy, p => p.RequireRole(Roles.AdminGroupName, Roles.ManagerGroupName));
});

/* **************************************************** */

var app = builder.Build();

// Initialize database: apply migrations and seed default roles on first start
using (var loggerFactory = LoggerFactory.Create(logging => logging.AddSerilog()))
{
    await DbInitializer.InitializeAsync(app.Services, loggerFactory.CreateLogger("DbInitializer"));
}

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Errors/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseRouting();

app.UseAuthentication();

// Capture status codes ONLY after authentication state has resolved
app.UseStatusCodePagesWithReExecute("/Errors/{0}");

app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");

app.Run();
