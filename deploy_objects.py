from snowflake.snowpark import Session
import os
import glob
import csv
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
from rich import print
from rich.console import Console
from rich.progress import Progress, TaskID
from rich.table import Table
from typing import List, Dict, Any
import argparse
import re
from snowflake.connector.util_text import split_statements
import io


class SnowflakeDeployer:
    def __init__(self, connection_name: str, database: str, max_workers: int = 5):
        self.connection_name = connection_name
        self.database = database
        self.max_workers = max_workers
        self.deploy_info = []
        self.deploy_lock = Lock()
        self.console = Console()
        self.failed_sql = []
        # Create session
        self.session = Session.builder.config("connection_name", connection_name).getOrCreate()
        self.session.sql(f"USE DATABASE {database}").show()
        
    def process_single_view(self, item_path: str, progress: Progress = None, task_id: TaskID = None) -> Dict[str, Any]:
        """
        Process a single view file and execute it in Snowflake.
        
        Args:
            view_path: Path to the view SQL file
            progress: Rich progress bar instance (optional)
            task_id: Progress task ID (optional)
            
        Returns:
            Dictionary with deployment information
        """
        item_name   = os.path.basename(item_path).replace(".sql", "")
        schema      = "unknown"
        last_sql  = ""
        try:
            # Read the SQL file
            with open(item_path, "r", encoding='utf-8') as file:
                sql = file.read()
            
            match = re.match(r'CREATE\s+(VIEW|SCHEMA|FORCE VIEW|TABLE)\s+"([^"]+)"\."([^"]+)"', sql, re.IGNORECASE)
            if not match:
                raise ValueError(f"Invalid SQL statement in {item_path}. Expected CREATE VIEW/SCHEMA/TABLE or similar.")
            item_type = match.group(1).upper()
            item_name = match.group(3)
            item_schema = match.group(2)
           
            
            table_info = {
                "schema": item_schema,
                "view_name": item_name,
                "status": "ok",
                "exception": None,
                "file_path": item_path,
                "thread_id": threading.current_thread().name
            }
            
            # Check if SQL is empty
            if sql.strip() == "":
                self.console.print(f"[yellow]Skipping empty SQL for view {item_name} in schema {item_schema}[/yellow]")
                table_info["status"] = "empty"
            else:
                self.console.print(f"[blue]Executing  {item_name} in schema {item_schema} (Thread: {threading.current_thread().name})[/blue]")
                count = 1
                for sqlitem, _ in split_statements(io.StringIO(sql), remove_comments=True):
                    # Execute the SQL
                    last_sql = sqlitem.strip()
                    self.session.sql(sqlitem).collect()
                    self.console.print(f"[green]✓ Successfully deployed {count} - {item_name} in schema {item_schema}[/green]")
                    count += 1
        except Exception as e:
            error_msg = str(e).replace("\n", " ")
            if "already exists" in error_msg.lower():
                self.console.print(f"[yellow]Object {item_name} already exists, skipping creation[/yellow]")
                table_info["status"] = "already"
                table_info["exception"] = error_msg
            else:            
                self.console.print(f"[red]✗ Error executing object {item_name} in schema {item_schema}: {error_msg}[/red]")
                table_info["status"] = "error"
                table_info["exception"] = error_msg
                if last_sql:
                    self.failed_sql.append({
                        "schema": item_schema,
                        "view_name": item_name,
                        "sql": last_sql,
                        "error": error_msg,
                        "file_path": item_path,
                        "thread_id": threading.current_thread().name
                    })

            
        finally:
            # Update progress if available
            if progress and task_id:
                progress.update(task_id, advance=1)
                
        return table_info
    
    def deploy_views_threaded(self, pattern: str, output_csv: str = "deploy_views_info.csv") -> List[Dict[str, Any]]:
        """
        Deploy views using multiple threads.
        
        Args:
            pattern: Glob pattern to find view files
            output_csv: Path to output CSV file
            
        Returns:
            List of deployment information dictionaries
        """
        # Find all view files
        views = glob.glob(pattern, recursive=True)
        
        if not views:
            self.console.print(f"[yellow]No view files found matching pattern: {pattern}[/yellow]")
            return []
        
        self.console.print(f"[blue]Found {len(views)} view files to deploy[/blue]")
        self.console.print(f"[blue]Using {self.max_workers} threads for deployment[/blue]")
        
        # Initialize progress tracking
        with Progress() as progress:
            task = progress.add_task("[green]Deploying views...", total=len(views))
            
            # Use ThreadPoolExecutor for concurrent execution
            with ThreadPoolExecutor(max_workers=self.max_workers, thread_name_prefix="ViewWorker") as executor:
                # Submit all tasks
                future_to_view = {
                    executor.submit(self.process_single_view, view, progress, task): view 
                    for view in views
                }
                
                # Collect results as they complete
                for future in as_completed(future_to_view):
                    view_path = future_to_view[future]
                    try:
                        result = future.result()
                        with self.deploy_lock:
                            self.deploy_info.append(result)
                    except Exception as exc:
                        self.console.print(f"[red]View {view_path} generated an exception: {exc}[/red]")
                        with self.deploy_lock:
                            self.deploy_info.append({
                                "schema": "unknown",
                                "view_name": os.path.basename(view_path).replace(".sql", ""),
                                "status": "error",
                                "exception": str(exc),
                                "file_path": view_path,
                                "thread_id": threading.current_thread().name
                            })
        
        # Write results to CSV
        self.write_deployment_report(output_csv)
        
        return self.deploy_info
    
    def write_deployment_report(self, output_csv: str):
        """Write deployment results to CSV file."""
        fieldnames = ["schema", "view_name", "status", "exception", "file_path", "thread_id"]
        
        try:
            with open(output_csv, "w", newline='', encoding='utf-8') as f:
                writer = csv.DictWriter(f, fieldnames=fieldnames)
                writer.writeheader()
                for info in self.deploy_info:
                    writer.writerow(info)
            
            self.console.print(f"[blue]Deployment report saved to: {output_csv}[/blue]")
            
        except Exception as e:
            self.console.print(f"[red]Error writing CSV report: {e}[/red]")
    
    def print_summary(self):
        """Print a summary table of deployment results."""
        if not self.deploy_info:
            self.console.print("[yellow]No deployment information available[/yellow]")
            return
        
        # Count results by status
        status_counts = {}
        for info in self.deploy_info:
            status = info["status"]
            status_counts[status] = status_counts.get(status, 0) + 1
        
        # Create summary table
        table = Table(title="Deployment Summary")
        table.add_column("Status", style="cyan")
        table.add_column("Count", style="magenta")
        table.add_column("Percentage", style="green")
        
        total = len(self.deploy_info)
        for status, count in status_counts.items():
            percentage = (count / total) * 100
            table.add_row(status.upper(), str(count), f"{percentage:.1f}%")
        
        table.add_row("TOTAL", str(total), "100.0%", style="bold")
        
        self.console.print(table)
        
        # Print error details if any
        errors = [info for info in self.deploy_info if info["status"] == "error"]
        if errors:
            self.console.print(f"\n[red]Found {len(errors)} errors:[/red]")
            for error in errors[:10]:  # Show first 10 errors
                self.console.print(f"  • {error['schema']}.{error['view_name']}: {error['exception']}")
            if len(errors) > 10:
                self.console.print(f"  ... and {len(errors) - 10} more errors (see CSV for full details)")


def main():
    parser = argparse.ArgumentParser(
        description="Deploy Snowflake views using multiple threads",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python deploy_views.py
  python deploy_views.py --pattern "views/**/*.sql" --threads 10
  python deploy_views.py --connection myconn --database MYDB --output results.csv
        """
    )
    
    parser.add_argument(
        '--pattern',
        default="",
        help='Glob pattern to find view files'
    )
    
    parser.add_argument(
        '--connection',
        default="",
        help='Snowflake connection name'
    )
    
    parser.add_argument(
        '--database',
        default="",
        help='Snowflake database name'
    )
    
    parser.add_argument(
        '--threads',
        type=int,
        default=5,
        help='Number of threads to use (default: 5)'
    )
    
    parser.add_argument(
        '--output',
        default="deploy_objects_info.csv",
        help='Output CSV file path'
    )
    
    args = parser.parse_args()
    
    try:
        # Create deployer instance
        deployer = SnowflakeDeployer(
            connection_name=args.connection,
            database=args.database,
            max_workers=args.threads
        )
        
        # Deploy views
        start_time = time.time()
        results = deployer.deploy_views_threaded(args.pattern, args.output)
        end_time = time.time()
        
        # Print results
        deployer.print_summary()
        
        duration = end_time - start_time
        deployer.console.print(f"\n[green]Deployment completed in {duration:.2f} seconds[/green]")
        
        # Return appropriate exit code
        errors = len([r for r in results if r["status"] == "error"])
        if errors > 0:
            deployer.console.print(f"[yellow]Completed with {errors} errors[/yellow]")
            return 1
        else:
            deployer.console.print("[green]All objects deployed successfully![/green]")
            return 0
            
    except KeyboardInterrupt:
        print("\n[yellow]Deployment interrupted by user[/yellow]")
        return 1
    except Exception as e:
        print(f"[red]Fatal error: {e}[/red]")
        return 1


if __name__ == "__main__":
    exit(main())