using System;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CSVWorker.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "IMDSDatabase",
                columns: table => new
                {
                    Id = table.Column<long>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    PartNumber = table.Column<string>(type: "TEXT", maxLength: 255, nullable: true),
                    ForsPN = table.Column<string>(type: "TEXT", maxLength: 255, nullable: true),
                    SIGIPPN = table.Column<string>(type: "TEXT", maxLength: 255, nullable: true),
                    VisualPN = table.Column<string>(type: "TEXT", maxLength: 255, nullable: true),
                    WGK = table.Column<string>(type: "TEXT", maxLength: 255, nullable: true),
                    NodeID = table.Column<string>(type: "TEXT", maxLength: 255, nullable: true),
                    CreatedAt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "CURRENT_TIMESTAMP"),
                    LastUpdatedAt = table.Column<DateTime>(type: "TEXT", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_IMDSDatabase", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "IMDSPorscheDatabase",
                columns: table => new
                {
                    Id = table.Column<long>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    PartNumber = table.Column<string>(type: "TEXT", maxLength: 255, nullable: true),
                    ArticleName = table.Column<string>(type: "TEXT", maxLength: 255, nullable: true),
                    MaterialGroup = table.Column<string>(type: "TEXT", maxLength: 255, nullable: true),
                    CrossSec = table.Column<string>(type: "TEXT", maxLength: 255, nullable: true),
                    CreatedAt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "CURRENT_TIMESTAMP"),
                    LastUpdatedAt = table.Column<DateTime>(type: "TEXT", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_IMDSPorscheDatabase", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Roles",
                columns: table => new
                {
                    Id = table.Column<long>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    Name = table.Column<string>(type: "TEXT", maxLength: 255, nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "CURRENT_TIMESTAMP")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Roles", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "Users",
                columns: table => new
                {
                    Id = table.Column<long>(type: "INTEGER", nullable: false)
                        .Annotation("Sqlite:Autoincrement", true),
                    Name = table.Column<string>(type: "TEXT", maxLength: 255, nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "timestamp", nullable: false, defaultValueSql: "CURRENT_TIMESTAMP"),
                    LastUpdatedAt = table.Column<DateTime>(type: "TEXT", nullable: true)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_Users", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "RoleUser",
                columns: table => new
                {
                    RolesId = table.Column<long>(type: "INTEGER", nullable: false),
                    UsersId = table.Column<long>(type: "INTEGER", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_RoleUser", x => new { x.RolesId, x.UsersId });
                    table.ForeignKey(
                        name: "FK_RoleUser_Roles_RolesId",
                        column: x => x.RolesId,
                        principalTable: "Roles",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                    table.ForeignKey(
                        name: "FK_RoleUser_Users_UsersId",
                        column: x => x.UsersId,
                        principalTable: "Users",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.CreateIndex(
                name: "IX_IMDSDatabase_ForsPN",
                table: "IMDSDatabase",
                column: "ForsPN");

            migrationBuilder.CreateIndex(
                name: "IX_IMDSDatabase_NodeID",
                table: "IMDSDatabase",
                column: "NodeID");

            migrationBuilder.CreateIndex(
                name: "IX_IMDSDatabase_PartNumber",
                table: "IMDSDatabase",
                column: "PartNumber");

            migrationBuilder.CreateIndex(
                name: "IX_IMDSDatabase_SIGIPPN",
                table: "IMDSDatabase",
                column: "SIGIPPN");

            migrationBuilder.CreateIndex(
                name: "IX_IMDSDatabase_VisualPN",
                table: "IMDSDatabase",
                column: "VisualPN");

            migrationBuilder.CreateIndex(
                name: "IX_IMDSDatabase_WGK",
                table: "IMDSDatabase",
                column: "WGK");

            migrationBuilder.CreateIndex(
                name: "IX_IMDSPorscheDatabase_ArticleName",
                table: "IMDSPorscheDatabase",
                column: "ArticleName");

            migrationBuilder.CreateIndex(
                name: "IX_IMDSPorscheDatabase_CrossSec",
                table: "IMDSPorscheDatabase",
                column: "CrossSec");

            migrationBuilder.CreateIndex(
                name: "IX_IMDSPorscheDatabase_MaterialGroup",
                table: "IMDSPorscheDatabase",
                column: "MaterialGroup");

            migrationBuilder.CreateIndex(
                name: "IX_IMDSPorscheDatabase_PartNumber",
                table: "IMDSPorscheDatabase",
                column: "PartNumber");

            migrationBuilder.CreateIndex(
                name: "IX_Roles_Name",
                table: "Roles",
                column: "Name",
                unique: true);

            migrationBuilder.CreateIndex(
                name: "IX_RoleUser_UsersId",
                table: "RoleUser",
                column: "UsersId");

            migrationBuilder.CreateIndex(
                name: "IX_Users_Name",
                table: "Users",
                column: "Name",
                unique: true);
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "IMDSDatabase");

            migrationBuilder.DropTable(
                name: "IMDSPorscheDatabase");

            migrationBuilder.DropTable(
                name: "RoleUser");

            migrationBuilder.DropTable(
                name: "Roles");

            migrationBuilder.DropTable(
                name: "Users");
        }
    }
}
