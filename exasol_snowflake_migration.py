#!/usr/bin/env python3
"""
Exasol to Snowflake View Migration Script

This script processes view files in a specified folder and applies syntax transformations
to migrate from Exasol to Snowflake SQL syntax.
"""

import os
import re
import glob
import argparse
import shutil
import csv
from datetime import datetime
from pathlib import Path
from typing import List, Tuple


def fix_final_comments(content: str) -> str:
    """
    Fix final comments in the SQL content by converting 'COMMENT IS' to 'SET COMMENT ='
    """
    match = re.match(r'''CREATE\s*(FORCE)?\s*VIEW\s+("[^"]+")\.("[^"]+")''', content, re.IGNORECASE)
    if not match:
        return False, content
    schema = match.group(2).strip('"')
    item_name = match.group(3).strip('"')
    match = re.match(r"(.*)\b(\s*COMMENT\s+(.*?);)$", content, re.IGNORECASE | re.DOTALL | re.MULTILINE)
    if match:
        content = match.group(1) + "\n\n" + match.group(2).strip()
    lines = content.splitlines()
    new_lines = []
    qualifies = False
    for line in lines:
        if line.strip().upper().startswith("COMMENT IS"):
                qualifies=True
                break
        elif re.match(r"\)\s+COMMENT\s+IS", line.strip(), re.IGNORECASE):
                qualifies=True
                break
    new_lines = []
    if qualifies:
            for i, line in enumerate(lines):
                if line.strip().upper().startswith("COMMENT IS"):
                    new_lines.append(";\n\nALTER VIEW " + schema + "." + item_name + " " + line.replace("COMMENT IS", "SET COMMENT =").replace("comment is","SET COMMENT ="))
                elif re.match(r"\)\s+COMMENT\s+IS", line.strip(), re.IGNORECASE):
                    line = re.sub(r"\)\s+COMMENT\s+IS", "SET COMMENT = ", line.strip(), count=1, flags=re.IGNORECASE)
                    new_lines.append(");\n\nALTER VIEW " + schema + "." + item_name + " " + line)
                else:
                    new_lines.append(line + "\n")
    else:
        content = re.sub(r"\b\s*COMMENT\s+IS\b", " COMMENT ", content, flags=re.IGNORECASE)
        return False, content
    
    # remove any missing COMMENT IS
    content = "".join(new_lines)
    content = re.sub(r"\b\s*COMMENT\s+IS\b", " COMMENT ", content, flags=re.IGNORECASE)
    return True, content 


def get_transformation_patterns() -> List[Tuple[str, str, str]]:
    """
    Returns a list of (pattern, replacement, description) tuples for syntax transformations.
    """
    return [
        # SYNTAX CHANGES
        (
            r'\bCREATE\s+FORCE\s+VIEW\s',
            r'CREATE VIEW ',
            "CREATE FORCE VIEW to CREATE VIEW"
        ),
        (
            r'\bCLOSE\s+SCHEMA\s*;',
            r'',
            "REMOVE CLOSE SCHEMA statement"
        ),
        (
            r'\bOPEN\s+SCHEMA\s+"?[^"]+"?\s*;',
            r'',
            "REMOVE OPEN SCHEMA statement"
        ),
        # TIME OPERATIONS
        (
            r'CONVERT_TZ\s*\(\s*SYSTIMESTAMP\s*,\s*DBTIMEZONE\s*,\s*\'UTC\'\s*\)',
            r"CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())",
            "CONVERT_TZ with SYSTIMESTAMP and DBTIMEZONE to CONVERT_TIMEZONE"
        ),
        (
            r'convert_tz\s*\(\s*current_timestamp\s*,\s*sessiontimezone\s*,\s*\'UTC\'\s*\)',
            r"CONVERT_TIMEZONE('UTC', CURRENT_TIMESTAMP())",
            "convert_tz with current_timestamp and sessiontimezone to CONVERT_TIMEZONE"
        ),
        (
            r'CONVERT_TZ\s*\(\s*(\w+)\s*,\s*(\'[^\']+\')\s*,\s*(\'[^\']+\')\s*\)',
            r'CONVERT_TIMEZONE(\2,\3,\1)',
            "Generic CONVERT_TZ to CONVERT_TIMEZONE with parameter reordering"
        ),
        (
            r'TRUNC\s*\(\s*(\w+)\s*\)',
            r"DATE_TRUNC('DAY',\1)",
            "TRUNC to DATE_TRUNC with DAY parameter"
        ),
        (
            r'\bSYSTIMESTAMP\b',
            r'CURRENT_TIMESTAMP()',
            "SYSTIMESTAMP to CURRENT_TIMESTAMP()"
        ),
        
        # ENCODING SPEC FOR VARCHAR
        (
            r'VARCHAR\s*(\(\s*\d+\s*\))\s*UTF8',
            r'VARCHAR\1',
            "Remove UTF8 encoding specification from VARCHAR"
        ),
        
        # COMMENTS FOR VIEW COLUMNS
        (
            r"(\s+)comment\s+is\s+('[^']+'),",
            r'\1COMMENT \2,',
            "Convert 'comment is' to 'COMMENT'"
        ),
        (
            r'''\s*,\s*("?\w+"?)\s+comment\s+is\s+('[^']+')''',
            r', \1 COMMENT \2',
            "Convert 'comment is' to 'COMMENT' 2"
        ),
        # HASHTYPE CASTS
        (
            r'CAST\s*\(\s*(\'[A-F0-9]+\')\s+AS\s+HASHTYPE\s*\)',
            r"TO_BINARY(\1, 'HEX')",
            "Convert CAST AS HASHTYPE to TO_BINARY with HEX"
        ),
        
        # REMOVE LOCAL. references (case sensitive and case insensitive)
        (
            r'\bLOCAL\.',
            r'',
            "Remove LOCAL. references (uppercase)"
        ),
        (
            r'\blocal\.',
            r'',
            "Remove local. references (lowercase)"
        ),
    ]


def apply_transformations(content: str, verbose: bool = False) -> Tuple[str, List[str]]:
    """
    Apply all transformation patterns to the content.
    
    Args:
        content: The original file content
        verbose: Whether to print detailed transformation info
        
    Returns:
        Tuple of (transformed_content, list_of_applied_transformations)
    """
    transformed_content = content
    applied_transformations = []
    patterns = get_transformation_patterns()
    
    for pattern, replacement, description in patterns:
        # Use re.IGNORECASE for case-insensitive matching where appropriate
        flags = re.IGNORECASE
        
        matches = re.findall(pattern, transformed_content, flags=flags)
        if matches:
            transformed_content = re.sub(pattern, replacement, transformed_content, flags=flags)
            applied_transformations.append(f"{description} ({len(matches)} occurrences)")
            
            if verbose:
                print(f"  ✓ {description}: {len(matches)} replacements")
    # Special case for final comments
    final_comment_fixed, transformed_content = fix_final_comments(transformed_content)
    if final_comment_fixed:
        applied_transformations.append("Fixed final comments (ALTER VIEW SET COMMENT)")
        if verbose:
            print("  ✓ Fixed final comments (ALTER VIEW SET COMMENT)")
    else:   
        if verbose:
            print("  No final comment changes needed")
    return transformed_content, applied_transformations


def find_view_files(folder_path: str, patterns: List[str] = None) -> List[str]:
    """
    Find all view files in the specified folder using glob patterns.
    
    Args:
        folder_path: Path to the folder containing view files
        patterns: List of glob patterns to match files (default: common view file patterns)
        
    Returns:
        List of file paths
    """
    if patterns is None:
        patterns = [
            "*.sql",
            "*.view",
            "*_view.sql",
            "view_*.sql",
            "*.ddl"
        ]
    
    view_files = []
    folder_path = Path(folder_path)
    
    for pattern in patterns:
        matches = glob.glob(str(folder_path / pattern), recursive=False)
        view_files.extend(matches)
    
    # Remove duplicates and sort
    return sorted(list(set(view_files)))


def process_file(file_path: str, backup: bool = True, verbose: bool = False) -> Tuple[bool, str, List[str]]:
    """
    Process a single file, applying all transformations.
    
    Args:
        file_path: Path to the file to process
        backup: Whether to create a backup of the original file
        verbose: Whether to print detailed information
        
    Returns:
        Tuple of (was_modified, status_message, applied_transformations)
    """
    try:
        # Read the original file
        with open(file_path, 'r', encoding='utf-8') as f:
            original_content = f.read()
        
        # Apply transformations
        transformed_content, applied_transformations = apply_transformations(original_content, verbose)
        
        # Check if any changes were made
        if transformed_content == original_content:
            if verbose:
                print(f"  No changes needed for {file_path}")
            return False, "No changes needed", []
        
        # Create backup if requested
        backup_path = ""
        if backup:
            backup_path = f"{file_path}.exasol_backup"
            shutil.copy2(file_path, backup_path)
            if verbose:
                print(f"  Created backup: {backup_path}")
        
        # Write the transformed content
        with open(file_path, 'w', encoding='utf-8') as f:
            f.write(transformed_content)
        
        print(f"✓ Processed: {file_path}")
        if verbose and applied_transformations:
            for transformation in applied_transformations:
                print(f"  - {transformation}")
        
        status_msg = f"Successfully modified ({len(applied_transformations)} transformations)"
        if backup_path:
            status_msg += f" - Backup: {os.path.basename(backup_path)}"
            
        return True, status_msg, applied_transformations
        
    except Exception as e:
        error_msg = f"Error processing file: {str(e)}"
        print(f"✗ Error processing {file_path}: {str(e)}")
        return False, error_msg, []


def write_csv_report(csv_path: str, file_records: List[dict]):
    """
    Write the processing results to a CSV file.
    
    Args:
        csv_path: Path to the CSV file to create
        file_records: List of dictionaries containing file processing information
    """
    fieldnames = [
        'file_path',
        'file_name', 
        'was_modified',
        'status',
        'transformations_applied',
        'transformation_count',
        'file_size_bytes',
        'processed_timestamp'
    ]
    
    try:
        with open(csv_path, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(file_records)
        
        print(f"CSV report written to: {csv_path}")
        
    except Exception as e:
        print(f"Error writing CSV report: {str(e)}")


def analyze_file_for_dry_run(file_path: str, verbose: bool = False) -> Tuple[bool, str, List[str]]:
    """
    Analyze a file for potential changes without modifying it.
    
    Args:
        file_path: Path to the file to analyze
        verbose: Whether to print detailed information
        
    Returns:
        Tuple of (would_be_modified, status_message, potential_transformations)
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
        
        transformed_content, applied_transformations = apply_transformations(content, verbose)
        
        if transformed_content != content:
            status_msg = f"Would be modified ({len(applied_transformations)} transformations)"
            if verbose:
                print(f"Would modify: {file_path}")
                if applied_transformations:
                    for transformation in applied_transformations:
                        print(f"  - {transformation}")
            return True, status_msg, applied_transformations
        else:
            if verbose:
                print(f"No changes needed: {file_path}")
            return False, "No changes needed", []
            
    except Exception as e:
        error_msg = f"Error analyzing file: {str(e)}"
        print(f"Error analyzing {file_path}: {str(e)}")
        return False, error_msg, []
    
def main():
    parser = argparse.ArgumentParser(
        description="Migrate Exasol view files to Snowflake syntax",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python migrate_views.py /path/to/views
  python migrate_views.py /path/to/views --no-backup --verbose
  python migrate_views.py /path/to/views --patterns "*.sql" "*.view"
  python migrate_views.py /path/to/views --csv-output my_report.csv
  python migrate_views.py /path/to/views --dry-run --csv-output analysis.csv
        """
    )
    
    parser.add_argument(
        'folder',
        help='Path to the folder containing view files'
    )
    
    parser.add_argument(
        '--patterns',
        nargs='+',
        default=None,
        help='Glob patterns to match view files (default: *.sql, *.view, *_view.sql, view_*.sql, *.ddl)'
    )
    
    parser.add_argument(
        '--csv-output',
        help='Path to CSV file for tracking processing results (default: migration_report_YYYYMMDD_HHMMSS.csv)'
    )
    
    parser.add_argument(
        '--no-backup',
        action='store_true',
        help='Do not create backup files'
    )

    parser.add_argument(
        '--backup',
        action='store_true',
        help='Create backup files'
    )
    
    parser.add_argument(
        '--verbose', '-v',
        action='store_true',
        help='Print detailed information about transformations'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Show what would be changed without actually modifying files'
    )
    
    args = parser.parse_args()
    args.no_backup = args.no_backup or not args.backup  # Normalize no-backup flag
    # Validate folder path
    if not os.path.isdir(args.folder):
        print(f"Error: '{args.folder}' is not a valid directory")
        return 1
    
    # Find view files
    print(f"Searching for view files in: {args.folder}")
    view_files = find_view_files(args.folder, args.patterns)
    
    if not view_files:
        print("No view files found matching the specified patterns")
        return 0
    
    print(f"Found {len(view_files)} view files")
    
    if args.verbose:
        print("Files to process:")
        for file_path in view_files:
            print(f"  - {file_path}")
        print()
    
    # Generate CSV output path if not provided
    if not args.csv_output:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        args.csv_output = f"migration_report_{timestamp}.csv"
    
    # Process files
    processed_count = 0
    modified_count = 0
    file_records = []
    processing_timestamp = datetime.now().isoformat()
    
    for file_path in view_files:
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        
        if args.dry_run:
            # For dry run, just analyze what would be changed
            was_modified, status_msg, applied_transformations = analyze_file_for_dry_run(file_path, args.verbose)
        else:
            # Actually process the file
            was_modified, status_msg, applied_transformations = process_file(file_path, not args.no_backup, args.verbose)
        
        # Create record for CSV
        file_record = {
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'was_modified': 'Yes' if was_modified else 'No',
            'status': status_msg,
            'transformations_applied': '; '.join(applied_transformations) if applied_transformations else '',
            'transformation_count': len(applied_transformations),
            'file_size_bytes': file_size,
            'processed_timestamp': processing_timestamp
        }
        
        file_records.append(file_record)
        processed_count += 1
        
        if was_modified:
            modified_count += 1
    
    # Write CSV report
    write_csv_report(args.csv_output, file_records)
    
    # Summary
    print(f"\nSummary:")
    print(f"  Files processed: {processed_count}")
    print(f"  Files modified: {modified_count}")
    print(f"  CSV report: {args.csv_output}")
    
    if args.dry_run:
        print(f"  (Dry run - no files were actually modified)")
    elif not args.no_backup and modified_count > 0:
        print(f"  Backup files created with .exasol_backup extension")
    
    return 0


if __name__ == "__main__":
    exit(main())
