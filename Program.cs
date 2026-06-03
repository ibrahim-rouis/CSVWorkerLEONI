using CSVWorker.Configuration;
using CSVWorker.Models;
using CSVWorker.Services;
using CSVWorker.Services.LDAP;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Authentication.Negotiate;
using Microsoft.AspNetCore.Authorization;
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

// LDAP
builder.Services.AddScoped<LdapService>(); // Your helper for LDAP

// Cache is used for caching LDAP roles
builder.Services.AddMemoryCache();

// ClaimsTransformation fetched user roles through LDAP and add them to user roles claims
builder.Services.AddTransient<IClaimsTransformation, LdapClaimsTransformer>();

// IMDS Services
builder.Services.AddScoped<IMDSMacrosService>();
builder.Services.AddScoped<IMDSDatabaseService>();

// Logs service
builder.Services.AddScoped<LogViewerService>();


/* ************* Custom configurations binding setup ***************** */

// Bind CsvWorkerConfig from appsettings.json to the CsvWorkerConfig class and make it available for injection
builder.Services.Configure<CSVWorkerConfig>(builder.Configuration.GetSection("CsvWorkerConfig"));

// Bind LdapConfig from appsettings.json
builder.Services.Configure<LdapConfig>(builder.Configuration.GetSection("LdapConfig"));

/* ************* Authentication Setup  ***************** */

var authBuilder = builder.Services.AddAuthentication(options =>
{
    options.DefaultScheme = NegotiateDefaults.AuthenticationScheme;
    options.DefaultAuthenticateScheme = NegotiateDefaults.AuthenticationScheme;
    options.DefaultChallengeScheme = NegotiateDefaults.AuthenticationScheme;
});
authBuilder.AddNegotiate();

/* **************************************************** */

builder.Services.AddAuthorization(options =>
{
    // Require authenticated user for all endpoints by default
    options.FallbackPolicy = new AuthorizationPolicyBuilder()
         .RequireAuthenticatedUser()
         .Build();
});

var app = builder.Build();

// Configure the HTTP request pipeline.
if (!app.Environment.IsDevelopment())
{
    app.UseExceptionHandler("/Home/Error");
    // The default HSTS value is 30 days. You may want to change this for production scenarios, see https://aka.ms/aspnetcore-hsts.
    app.UseHsts();
}
app.UseStatusCodePagesWithReExecute("/Home/Status/{0}");

// Intercept HTTP errors and route them to the Home Controller's NotFound action
//app.UseStatusCodePagesWithReExecute("/NotFound");

app.UseHttpsRedirection();
app.UseStaticFiles();
app.UseRouting();

app.UseAuthentication();

app.UseAuthorization();

app.MapControllerRoute(
    name: "default",
    pattern: "{controller=Home}/{action=Index}/{id?}");

app.Run();
