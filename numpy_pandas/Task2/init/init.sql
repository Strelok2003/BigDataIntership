CREATE DATABASE Test;
GO

USE Test;
GO

CREATE TABLE [dbo].[Materials](
	[year] [smallint] NULL,
	[month] [tinyint] NULL,
	[produced_material] [nvarchar](50) NULL,
	[produced_material_production_type] [nvarchar](50) NULL,
	[produced_material_release_type] [nvarchar](50) NULL,
	[produced_material_quantity] [nvarchar](50) NULL,
	[component_material] [nvarchar](50) NULL,
	[component_material_production_type] [nvarchar](50) NULL,
	[component_material_release_type] [nvarchar](50) NULL,
	[component_material_quantity] [nvarchar](50) NULL,
	[plant_id] [nvarchar](50) NULL
)
GO

BULK INSERT Materials
FROM '/data/task_2_data.csv'
WITH (
    FIRSTROW = 2,
    FIELDTERMINATOR = '\t',
    FIELDQUOTE = '"',
    ROWTERMINATOR = '\n',
    TABLOCK
);
GO