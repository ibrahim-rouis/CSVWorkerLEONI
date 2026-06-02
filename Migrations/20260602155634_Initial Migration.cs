using System;
using Microsoft.EntityFrameworkCore.Metadata;
using Microsoft.EntityFrameworkCore.Migrations;

#nullable disable

namespace CSVWorker.Migrations
{
    /// <inheritdoc />
    public partial class InitialMigration : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.AlterDatabase()
                .Annotation("MySql:CharSet", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "IMDSDatabase",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("MySql:ValueGenerationStrategy", MySqlValueGenerationStrategy.IdentityColumn),
                    PartNumber = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    ForsPN = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    SIGIPPN = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    VisualPN = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    WGK = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    NodeID = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    CreatedAt = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    LastUpdatedAt = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    createdBy = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    LastUpdatedBy = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_IMDSDatabase", x => x.Id);
                })
                .Annotation("MySql:CharSet", "utf8mb4");

            migrationBuilder.CreateTable(
                name: "IMDSPorscheDatabase",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("MySql:ValueGenerationStrategy", MySqlValueGenerationStrategy.IdentityColumn),
                    PartNumber = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    ArticleName = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    MaterialGroup = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    CrossSec = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    CreatedAt = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    LastUpdatedAt = table.Column<DateTime>(type: "datetime(6)", nullable: false),
                    createdBy = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4"),
                    LastUpdatedBy = table.Column<string>(type: "varchar(255)", maxLength: 255, nullable: true)
                        .Annotation("MySql:CharSet", "utf8mb4")
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_IMDSPorscheDatabase", x => x.Id);
                })
                .Annotation("MySql:CharSet", "utf8mb4");

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
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "IMDSDatabase");

            migrationBuilder.DropTable(
                name: "IMDSPorscheDatabase");
        }
    }
}
