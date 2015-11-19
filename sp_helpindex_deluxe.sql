USE [DBAUtility]
GO

IF OBJECT_ID(N'dbo.sp_helpindex_deluxe', N'P') IS NULL
BEGIN
    EXEC ('CREATE PROCEDURE dbo.sp_helpindex_deluxe AS RAISERROR(''There is no implementation for this sproc!'', 16, 1)')
END
GO

ALTER PROCEDURE dbo.sp_helpindex_deluxe 
(
	@object_name NVARCHAR(256),
	@database_name NVARCHAR(256) = NULL,
	@force_fill_factor BIT = 0,
	@new_fill_factor INT = NULL,
	@include_physical_stats BIT = 0
)
AS
BEGIN
	-- ====================================================================================================================================================    
	-- Author:			Brad Hurst (@bradtoast)
	--
	-- Create date:		11/17/2015
	--
	-- Description:		An adaptation of sp_helpindex3 by Jared Dobson and Greg Wright found at https://gist.github.com/onesupercoder/1604210, 
	--					which was an adaptation of sp_helpindex2 by Kimberly Tripp.
	--					Does not have to be installed on master and marked as a system object. in fact it shouldn't.  I include this in my DBAUtility database.
	--					Includes REBUILD, REORGANIZE AND DELETE INDEX syntax for all indexes returned. (provided by sp_helpindex3)
	--					Includes an option to include physical index stats.
	--
	--	Parameters:		@object_name			-> 1 part object name or 3 part name
	--					@database_name			-> if 3 part object name is not provided, this is required
	--					@force_fill_factor		-> if set to 1 will include the existing fillfactor or the @new_fill_factor value in the REBUILD syntax
	--					@new_fill_factor		-> if you want to include a new fillfactor in the REBUILD syntax and the @force_fill_factor is set to 1
	--					@include_physical_stats -> WARNING: THIS OPTION SLOWS DOWN THE EXECUTION TIME CONSIDERABLY! setting this option to 1 will include 
	--												index fragmentation information from sys.dm_db_index_physical_stats()
	-- ====================================================================================================================================================    
	SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED ;
	SET NOCOUNT ON ;
	
	IF @database_name IS NULL
	BEGIN	
		SET @database_name = PARSENAME(@object_name, 3)
		SET @object_name = PARSENAME(@object_name, 1)
	END
		
	IF @database_name IS NULL
	BEGIN
	    RAISERROR('@object_name does not contain full 3 part name or @database_name was not supplied. Please use a 3 part name for the @object_name (<database>.<schema>.<object_name>) or include a value for @database_name', 16, 1)
	END

	IF (OBJECT_ID(N'tempdb..#TempIndexes') IS NOT NULL) DROP TABLE #TempIndexes ;
	CREATE TABLE [dbo].[#TempIndexes]
	(
	[object_id] [int] NOT NULL,
	[index_id] [int] NOT NULL,
	[type_desc] NVARCHAR(256),
	[data_space_id] [int] NOT NULL,
	[data_space_name] NVARCHAR(256) NULL,
	[name] [sys].[sysname] NULL,
	[ignore_dup_key] [bit] NULL,
	[is_unique] [bit] NULL,
	[filter_definition] [nvarchar] (max) NULL,
	[is_hypothetical] [bit] NULL,
	[is_primary_key] [bit] NULL,
	[is_unique_constraint] [bit] NULL,
	[auto_created] [bit] NULL,
	[no_recompute] [bit] NULL,
	[last_user_seek] [datetime] NULL,
	[last_user_scan] [datetime] NULL,
	[last_user_lookup] [datetime] NULL,
	[last_user_update] [datetime] NULL,
	[user_seeks] [bigint] NULL,
	[user_scans] [bigint] NULL,
	[user_lookups] [bigint] NULL,
	[user_updates] [bigint] NULL,
	[fill_factor] [tinyint] NOT NULL,
	[key_columns] NVARCHAR(MAX) NULL,
	[included_columns] NVARCHAR(MAX) NULL
	) ;

	PRINT '@database_name: ' + @database_name

	DECLARE @sql NVARCHAR(MAX) = N'
		SELECT  
				i.object_id,
				i.index_id,  
				i.type_desc,
				i.data_space_id,  
				ds.name AS data_space_name,
				i.name,  
				i.ignore_dup_key,  
				i.is_unique,  
				i.filter_definition, 
				i.is_hypothetical,  
				i.is_primary_key,  
				i.is_unique_constraint, 
				s.auto_created,  
				s.no_recompute,  
				[DDIUS].[last_user_seek],  
				[DDIUS].[last_user_scan],  
				[DDIUS].[last_user_lookup],  
				[DDIUS].[last_user_update],  
				[DDIUS].[user_seeks],  
				[DDIUS].[user_scans],  
				[DDIUS].[user_lookups],  
				[DDIUS].[user_updates],  
				i.fill_factor ,
				KC.KEY_COLUMNS ,
				IC.INCLUDED_COLUMNS
		FROM ' + @database_name + '.sys.indexes i 
			INNER JOIN ' + @database_name + '.sys.stats s ON s.object_id = i.object_id AND s.stats_id = i.index_id
			INNER JOIN ' + @database_name + '.sys.data_spaces ds ON ds.data_space_id = i.data_space_id
			OUTER APPLY ( SELECT  
								MAX([DDI].[last_user_seek]) AS [last_user_seek],  
								MAX([DDI].[last_user_scan]) AS [last_user_scan],  
								MAX([DDI].[last_user_lookup]) AS [last_user_lookup],  
								MAX([DDI].[last_user_update]) AS [last_user_update],  
								SUM(ISNULL([DDI].[user_seeks], 0)) AS [user_seeks],  
								SUM(ISNULL([DDI].[user_scans], 0)) AS [user_scans],  
								SUM(ISNULL([DDI].[user_lookups], 0)) AS [user_lookups],  
								SUM(ISNULL([DDI].[user_updates], 0)) AS [user_updates]  
							  FROM  
								' + @database_name + '.[sys].[dm_db_index_usage_stats] AS DDI  
							  WHERE  
								[i].[index_id] = [DDI].[index_id]  
								AND [DDI].[object_id] = [i].[object_id]  
							  GROUP BY  
								[DDI].[object_id],  
								[DDI].[index_id]  
							) AS [DDIUS]
			OUTER APPLY ( SELECT (
						SELECT  c.name + ' + CHAR(39) + ', ' + CHAR(39) + ' 
						FROM    ' + @database_name + '.sys.index_columns ic
								INNER JOIN ' + @database_name + '.sys.columns AS c ON c.column_id = ic.column_id AND c.object_id = ic.object_id
						WHERE   key_ordinal > 0
								AND ic.object_id = i.object_id
								AND i.index_id = ic.index_id
						ORDER BY ic.index_id, ic.key_ordinal 
						FOR XML PATH (' + CHAR(39) + CHAR(39) +')) AS KEY_COLUMNS
						) AS KC
			OUTER APPLY ( SELECT (
						SELECT  c.name + ' + CHAR(39) + ', ' + CHAR(39) + ' 
						FROM    ' + @database_name + '.sys.index_columns ic
								INNER JOIN ' + @database_name + '.sys.columns AS c ON c.column_id = ic.column_id AND c.object_id = ic.object_id
						WHERE   ic.is_included_column = 1
								AND ic.object_id = i.object_id
								AND i.index_id = ic.index_id
						ORDER BY ic.index_id, ic.key_ordinal 
						FOR XML PATH (' + CHAR(39) + CHAR(39) +')) AS INCLUDED_COLUMNS
						) AS IC
						
		WHERE i.[object_id] = OBJECT_ID(N''' + @database_name + '.dbo.' + @object_name + ''')' 


	INSERT INTO #TempIndexes
	EXEC (@sql)

	IF (@include_physical_stats = 0)
	BEGIN
		SELECT  index_name			= t.name
			  , index_id			= t.index_id
			  , orig_fillfactor		= t.fill_factor
			  , index_description	= t.type_desc + 
										CASE WHEN t.is_unique  = 1 THEN ', UNIQUE' ELSE '' END +  
										CASE WHEN t.is_primary_key = 1 THEN ', PRIMARY KEY' ELSE '' END + 
										' located on ' + t.data_space_name
			  , index_key_columns	= CASE WHEN LEN(t.key_columns) > 0 THEN LEFT(t.key_columns, LEN(t.key_columns) - 1) ELSE '' END
			  , included_columns	= CASE WHEN LEN(t.included_columns) > 0 THEN LEFT(t.included_columns, LEN(t.included_columns) - 1) ELSE '' END 
			  , t.filter_definition 
			  , t.last_user_seek
			  , t.last_user_scan
			  , t.last_user_lookup
			  , t.last_user_update
			  , t.user_seeks
			  , t.user_scans
			  , t.user_lookups
			  , t.user_updates
			  , rebuild_text		= 'ALTER INDEX [' + t.name + '] ON [' +  @object_name + '] REBUILD WITH (ONLINE=ON' + 
										CASE WHEN @new_fill_factor = 1 THEN ', FILLFACTOR = ' + CASE WHEN @new_fill_factor IS NOT NULL THEN CONVERT(NVARCHAR(3), @new_fill_factor) ELSE CONVERT(NVARCHAR(3), t.fill_factor) END ELSE '' END + ') ;'
			  , reorganize_text		= 'ALTER INDEX [' + t.name + '] ON [' +  @object_name + '] REORGANIZE ;' 
			  , drop_text			= 'DROP INDEX  [' + t.name + '].[' +  @object_name + '] ;' 
		FROM  #TempIndexes AS t
	END
	ELSE 
	BEGIN
		SELECT  index_name			= t.name
			  , index_id			= t.index_id
			  , orig_fillfactor		= t.fill_factor
			  , index_description	= t.type_desc + 
										CASE WHEN t.is_unique  = 1 THEN ', UNIQUE' ELSE '' END +  
										CASE WHEN t.is_primary_key = 1 THEN ', PRIMARY KEY' ELSE '' END + 
										' located on ' + t.data_space_name
			  , index_key_columns	= CASE WHEN LEN(t.key_columns) > 0 THEN LEFT(t.key_columns, LEN(t.key_columns) - 1) ELSE '' END
			  , included_columns	= CASE WHEN LEN(t.included_columns) > 0 THEN LEFT(t.included_columns, LEN(t.included_columns) - 1) ELSE '' END 
			  , t.filter_definition
			  , t.last_user_seek
			  , t.last_user_scan
			  , t.last_user_lookup
			  , t.last_user_update
			  , t.user_seeks
			  , t.user_scans
			  , t.user_lookups
			  , t.user_updates
			  , rebuild_text		= 'ALTER INDEX [' + t.name + '] ON [' +  @object_name + '] REBUILD WITH (ONLINE=ON' + 
										CASE WHEN @new_fill_factor = 1 THEN ', FILLFACTOR = ' + CASE WHEN @new_fill_factor IS NOT NULL THEN CONVERT(NVARCHAR(3), @new_fill_factor) ELSE CONVERT(NVARCHAR(3), t.fill_factor) END ELSE '' END + ') ;'
			  , reorganize_text		= 'ALTER INDEX [' + t.name + '] ON [' +  @object_name + '] REORGANIZE ;' 
			  , drop_text			= 'DROP INDEX  [' + t.name + '].[' +  @object_name + '] ;' 
			  , f.avg_fragmentation_in_percent
			  , f.fragment_count
			  , f.avg_fragment_size_in_pages
			  , f.page_count
		FROM   master.sys.dm_db_index_physical_stats(DB_ID(@database_name), OBJECT_ID(@object_name), NULL, NULL, NULL) AS f
				INNER JOIN #TempIndexes AS t ON t.index_id = f.index_id AND t.object_id = f.object_id
		WHERE f.alloc_unit_type_desc = 'IN_ROW_DATA'
	END
END
GO
