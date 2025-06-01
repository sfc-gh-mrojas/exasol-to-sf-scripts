# Exasol to Snowflake View Migration Script

A Python script that automatically converts Exasol SQL view syntax to Snowflake-compatible syntax. The script processes multiple files in a directory, applies syntax transformations, creates backups, and generates detailed CSV reports of all changes made.

## Overview

When migrating database views from Exasol to Snowflake, several syntax differences need to be addressed. This script automates the conversion process by applying predefined transformation rules and tracking all changes made to your view files.

### Key Features

- **Batch Processing**: Processes multiple view files in a directory using glob patterns
- **Comprehensive Transformations**: Handles time operations, data types, comments, and Exasol-specific syntax
- **Safety First**: Creates backup files before making changes
- **Detailed Reporting**: Generates CSV reports tracking all processed files and applied transformations
- **Flexible Options**: Supports dry-run mode, custom file patterns, and verbose output
- **Error Handling**: Gracefully handles errors and continues processing other files

## Supported Transformations

The script applies the following syntax conversions:

### Time Operations
- `CONVERT_TZ(SYSTIMESTAMP, DBTIMEZONE, 'UTC')` → `CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())`
- `convert_tz(current_timestamp, sessiontimezone, 'UTC')` → `CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())`
- `CONVERT_TZ(column, 'from_tz', 'to_tz')` → `CONVERT_TIMEZONE('from_tz','to_tz',column)`
- `TRUNC(column)` → `DATE_TRUNC('DAY',column)`
- `SYSTIMESTAMP` → `CURRENT_TIMESTAMP()`

### Data Types
- `VARCHAR(n) UTF8` → `VARCHAR(n)` (removes UTF8 encoding specification)
- `CAST('hexvalue' AS HASHTYPE)` → `TO_BINARY('hexvalue', 'HEX')`

### Comments
- `comment is 'text',` → `COMMENT 'text',`

### Exasol-Specific Syntax
- Removes `LOCAL.` and `local.` references (Exasol's implicit row alias)

## Installation

### Requirements
- Python 3.6 or higher
- No external dependencies (uses only Python standard library)

### Setup
1. Download the script: `exasol_snowflake_migration.py`
2. Make it executable: `chmod +x exasol_snowflake_migration.py`

## Usage

### Basic Syntax
```bash
python exasol_snowflake_migration.py <folder_path> [options]
```

### Command Line Options

| Option | Description |
|--------|-------------|
| `folder` | **Required.** Path to the folder containing view files |
| `--patterns PATTERN [PATTERN ...]` | Glob patterns to match view files (default: `*.sql`, `*.view`, `*_view.sql`, `view_*.sql`, `*.ddl`) |
| `--csv-output PATH` | Path to CSV report file (default: auto-generated with timestamp) |
| `--no-backup` | Skip creating backup files |
| `--verbose, -v` | Print detailed transformation information |
| `--dry-run` | Analyze files without making changes |
| `--help, -h` | Show help message |

### Examples

#### Basic Usage
```bash
# Process all view files in a directory with default settings
python exasol_snowflake_migration.py /path/to/views
```

#### Custom File Patterns
```bash
# Only process specific file types
python exasol_snowflake_migration.py /path/to/views --patterns "*.sql" "*.ddl"
```

#### Dry Run Analysis
```bash
# See what would be changed without modifying files
python exasol_snowflake_migration.py /path/to/views --dry-run --verbose
```

#### Custom CSV Report
```bash
# Specify custom CSV output file
python exasol_snowflake_migration.py /path/to/views --csv-output my_migration_report.csv
```

#### No Backups + Verbose
```bash
# Skip backups and show detailed output
python exasol_snowflake_migration.py /path/to/views --no-backup --verbose
```

## Output Files

### Backup Files
- Created automatically unless `--no-backup` is used
- Named with `.exasol_backup` extension
- Example: `user_view.sql.exasol_backup`

### CSV Report
The script generates a comprehensive CSV report with the following columns:

| Column | Description |
|--------|-------------|
| `file_path` | Full path to the processed file |
| `file_name` | Just the filename |
| `was_modified` | "Yes" or "No" indicating if file was changed |
| `status` | Detailed status message |
| `transformations_applied` | Semicolon-separated list of applied transformations |
| `transformation_count` | Number of transformations applied |
| `file_size_bytes` | Original file size |
| `processed_timestamp` | When processing occurred |

### Sample CSV Output
```csv
file_path,file_name,was_modified,status,transformations_applied,transformation_count,file_size_bytes,processed_timestamp
/views/user_view.sql,user_view.sql,Yes,Successfully modified (3 transformations),SYSTIMESTAMP to CURRENT_TIMESTAMP() (2 occurrences); Remove LOCAL. references (1 occurrences),3,2048,2025-05-31T10:30:45.123456
/views/simple_view.sql,simple_view.sql,No,No changes needed,,0,1024,2025-05-31T10:30:45.123456
```

## Workflow Recommendations

### 1. Analysis Phase
```bash
# First, run a dry-run to see what will be changed
python exasol_snowflake_migration.py /path/to/views --dry-run --verbose --csv-output analysis.csv
```

### 2. Review Phase
- Review the generated CSV report
- Check the console output for any error messages
- Verify the transformations look correct

### 3. Migration Phase
```bash
# Run the actual migration
python exasol_snowflake_migration.py /path/to/views --verbose --csv-output migration_report.csv
```

### 4. Verification Phase
- Compare original files with backups if needed
- Test the modified views in Snowflake
- Use the CSV report to track which files were changed

## Error Handling

The script includes robust error handling:
- Individual file errors don't stop processing of other files
- All errors are logged in the CSV report
- Detailed error messages are displayed in the console
- Backup files are created before any modifications

## File Pattern Matching

### Default Patterns
- `*.sql` - Standard SQL files
- `*.view` - View definition files
- `*_view.sql` - Files ending with "_view.sql"
- `view_*.sql` - Files starting with "view_"
- `*.ddl` - Data Definition Language files

### Custom Patterns
You can specify your own patterns using the `--patterns` option:
```bash
python exasol_snowflake_migration.py /path/to/views --patterns "my_view_*.sql" "*.view"
```

## Troubleshooting

### Common Issues

1. **Permission Errors**: Ensure you have read/write access to the target directory
2. **File Encoding**: The script assumes UTF-8 encoding
3. **Large Files**: Very large files may take longer to process
4. **Pattern Matching**: Use quotes around patterns that contain special characters

### Verification Steps

1. Check the CSV report for any files marked with errors
2. Verify backup files were created (unless `--no-backup` was used)
3. Test a few converted files manually in Snowflake
4. Use `--dry-run` first to preview changes

## Contributing

To add new transformation rules:
1. Add the pattern to the `get_transformation_patterns()` function
2. Follow the format: `(regex_pattern, replacement, description)`
3. Test with sample files before running on production data

## License

This script is provided as-is for database migration purposes. Test thoroughly before using on production data.