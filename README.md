# icinga_check_mysql_schema_stats

This plugin recieves per-table usage statistics for specified schema.

## Minimal user privileges, requied to run check
For MySQL 8.0
```mysql
GRANT SELECT ON `performance_schema`.`file_summary_by_instance` TO `username`@`host`
GRANT SELECT ON `performance_schema`.`table_io_waits_summary_by_table` TO `username`@`host`
GRANT SELECT ON `sys`.`schema_table_statistics` TO `username`@`host`
GRANT SELECT ON `sys`.`x$ps_schema_table_statistics_io` TO `username`@`host`
GRANT EXECUTE ON FUNCTION `sys`.`extract_schema_from_file_name` TO `username`@`host`
GRANT EXECUTE ON FUNCTION `sys`.`extract_table_from_file_name` TO `username`@`host`
```

For MySQL 5.7 add following
```mysql
GRANT EXECUTE ON FUNCTION "sys"."format_time" TO `username`@`host`
GRANT EXECUTE ON FUNCTION "sys"."format_bytes" TO `username`@`host`
```
