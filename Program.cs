using CSVWorker.Configuration;
using CSVWorker.Models;
using CSVWorker.Security;
using CSVWorker.Services;
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

// Register the DbContext with the connection string and MySQL provider
var connectionString = builder.Configuration.GetConnectionString("MySqlConnection");
builder.Services.AddDbContext<CSVWorkerDBContext>(options =>
    options.UseMySql(connectionString, ServerVersion.AutoDetect(connectionString)));

builder.Services.AddSingleton<IHttpContextAccessor, HttpContextAccessor>();

// Add services to the container.
builder.Services.AddControllersWithViews();

/* ************* Services ***************** */

// Cache is used for caching LDAP roles
builder.Services.AddMemoryCache();

// IMDS Services
builder.Services.AddScoped<IMDSMacrosService>();
builder.Services.AddScoped<IMDSDatabaseService>();
builder.Services.AddScoped<IMDSPorscheDatabaseService>();

// Logs service
builder.Services.AddScoped<LogViewerService>();


/* ************* Custom configurations binding setup ***************** */

// Bind CsvWorkerConfig from appsettings.json to the CsvWorkerConfig class and make it available for injection
builder.Services.Configure<CSVWorkerConfig>(builder.Configuration.GetSection("CsvWorkerConfig"));

/* ************* Authentication Setup  ***************** */

builder.Services.AddAuthentication(NegotiateDefaults.AuthenticationScheme)
   .AddNegotiate();


// In production, enforce role-based authorization. 
if (!builder.Environment.IsDevelopment())
{
    builder.Services.AddAuthorization(options =>
    {
        options.FallbackPolicy = options.DefaultPolicy;
        options.AddPolicy(Policies.AdminPolicy, p => p.RequireRole(Roles.AdminGroupName));
        options.AddPolicy(Policies.ManagerPolicy, p => p.RequireRole(Roles.ManagerGroupName));
        options.AddPolicy(Policies.AdminOrManagerPolicy, p => p.RequireRole(Roles.AdminGroupName, Roles.ManagerGroupName));
    });
}
// In development, allow user without roles to access everything in policies
else
{
    builder.Services.AddAuthorization(options =>
    {
        options.FallbackPolicy = options.DefaultPolicy;
        options.AddPolicy(Policies.AdminPolicy, p => p.RequireAssertion(_ => true));
        options.AddPolicy(Policies.ManagerPolicy, p => p.RequireAssertion(_ => true));
        options.AddPolicy(Policies.AdminOrManagerPolicy, p => p.RequireAssertion(_ => true));
    });
}

/* **************************************************** */

var app = builder.Build();

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
