USE MyDB

EXEC sys.sp_cdc_enable_table
@source_schema = N'inventory',
@source_name   = N'customers',
@role_name     = NULL,
@supports_net_changes = 0
GO

EXEC sys.sp_cdc_help_change_data_capture
GO
