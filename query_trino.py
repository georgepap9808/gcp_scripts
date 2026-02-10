#!/usr/bin/env python3
"""
Trino Data Lake Query Script

This script provides functionality to query data lake tables (Delta Lake, Hudi, Iceberg)
using Trino SQL query engine.

Usage:
    python query_trino.py --catalog osd_delta_lake_catalog --schema kafka_test_delta --table iot_events --query "SELECT COUNT(*) FROM iot_events"
    python query_trino.py --catalog osd_delta_lake_catalog --list-schemas
    python query_trino.py --catalog osd_delta_lake_catalog --schema kafka_test_delta --list-tables
"""

import argparse
import sys
from typing import Optional, List, Dict, Any
from trino.dbapi import connect
from trino.auth import BasicAuthentication
import pandas as pd


class TrinoQueryClient:
    """Client for querying Trino data lake catalogs"""
    
    def __init__(
        self,
        host: str = "localhost",
        port: int = 8082,
        user: str = "trino",
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        password: Optional[str] = None,
    ):
        """
        Initialize Trino connection
        
        Args:
            host: Trino server hostname
            port: Trino server port (default: 8082)
            user: Trino username
            catalog: Default catalog name
            schema: Default schema name
            password: Optional password for authentication
        """
        self.host = host
        self.port = port
        self.user = user
        self.catalog = catalog
        self.schema = schema
        self.password = password
        
    def _get_connection(self, catalog: Optional[str] = None, schema: Optional[str] = None):
        """Create a Trino database connection"""
        catalog = catalog or self.catalog
        schema = schema or self.schema
        
        if not catalog:
            raise ValueError("Catalog must be specified either in constructor or query")
        
        auth = None
        if self.password:
            auth = BasicAuthentication(self.user, self.password)
        
        conn = connect(
            host=self.host,
            port=self.port,
            user=self.user,
            catalog=catalog,
            schema=schema,
            auth=auth,
        )
        return conn
    
    def execute_query(
        self,
        query: str,
        catalog: Optional[str] = None,
        schema: Optional[str] = None,
        return_pandas: bool = True,
    ) -> Optional[pd.DataFrame]:
        """
        Execute a SQL query against Trino
        
        Args:
            query: SQL query string
            catalog: Catalog name (uses default if not specified)
            schema: Schema name (uses default if not specified)
            return_pandas: If True, return results as pandas DataFrame
            
        Returns:
            Query results as pandas DataFrame if return_pandas=True, else None
        """
        conn = self._get_connection(catalog, schema)
        cur = conn.cursor()
        
        try:
            cur.execute(query)
            results = cur.fetchall()
            columns = [desc[0] for desc in cur.description] if cur.description else []
            
            if return_pandas and results:
                df = pd.DataFrame(results, columns=columns)
                return df
            elif return_pandas:
                return pd.DataFrame(columns=columns)
            else:
                return results
        except Exception as e:
            print(f"Error executing query: {e}", file=sys.stderr)
            raise
        finally:
            cur.close()
            conn.close()
    
    def list_schemas(self, catalog: str) -> List[str]:
        """
        List all schemas in a catalog
        
        Args:
            catalog: Catalog name
            
        Returns:
            List of schema names
        """
        query = "SHOW SCHEMAS"
        conn = self._get_connection(catalog)
        cur = conn.cursor()
        
        try:
            cur.execute(query)
            schemas = [row[0] for row in cur.fetchall()]
            return schemas
        finally:
            cur.close()
            conn.close()
    
    def list_tables(self, catalog: str, schema: str) -> List[str]:
        """
        List all tables in a schema
        
        Args:
            catalog: Catalog name
            schema: Schema name
            
        Returns:
            List of table names
        """
        query = f'SHOW TABLES FROM "{catalog}"."{schema}"'
        conn = self._get_connection(catalog, schema)
        cur = conn.cursor()
        
        try:
            cur.execute(query)
            tables = [row[0] for row in cur.fetchall()]
            return tables
        finally:
            cur.close()
            conn.close()
    
    def describe_table(self, catalog: str, schema: str, table: str) -> pd.DataFrame:
        """
        Get table schema information
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            
        Returns:
            DataFrame with column information
        """
        query = f'DESCRIBE "{catalog}"."{schema}"."{table}"'
        return self.execute_query(query, catalog, schema)
    
    def get_table_row_count(self, catalog: str, schema: str, table: str) -> int:
        """
        Get the number of rows in a table
        
        Args:
            catalog: Catalog name
            schema: Schema name
            table: Table name
            
        Returns:
            Number of rows
        """
        query = f'SELECT COUNT(*) as row_count FROM "{catalog}"."{schema}"."{table}"'
        df = self.execute_query(query, catalog, schema)
        return df.iloc[0]['row_count'] if df is not None and not df.empty else 0


def main():
    parser = argparse.ArgumentParser(
        description="Query data lake tables using Trino",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # List all schemas in a catalog
  python query_trino.py --catalog osd_delta_lake_catalog --list-schemas

  # List all tables in a schema
  python query_trino.py --catalog osd_delta_lake_catalog --schema kafka_test_delta --list-tables

  # Execute a query
  python query_trino.py --catalog osd_delta_lake_catalog --schema kafka_test_delta --table iot_events --query "SELECT COUNT(*) FROM iot_events"

  # Get table schema
  python query_trino.py --catalog osd_delta_lake_catalog --schema kafka_test_delta --table iot_events --describe

  # Get row count
  python query_trino.py --catalog osd_delta_lake_catalog --schema kafka_test_delta --table iot_events --count

  # Query with full table path
  python query_trino.py --catalog osd_delta_lake_catalog --query "SELECT * FROM kafka_test_delta.iot_events LIMIT 10"
        """
    )
    
    parser.add_argument(
        "--host",
        default="localhost",
        help="Trino server hostname (default: localhost)"
    )
    parser.add_argument(
        "--port",
        type=int,
        default=8082,
        help="Trino server port (default: 8082)"
    )
    parser.add_argument(
        "--user",
        default="trino",
        help="Trino username (default: trino)"
    )
    parser.add_argument(
        "--password",
        help="Trino password (optional)"
    )
    parser.add_argument(
        "--catalog",
        required=True,
        help="Trino catalog name (e.g., osd_delta_lake_catalog, osd_hudi_catalog, osd_iceberg_catalog)"
    )
    parser.add_argument(
        "--schema",
        help="Schema name (optional if specified in query)"
    )
    parser.add_argument(
        "--table",
        help="Table name (optional if specified in query)"
    )
    parser.add_argument(
        "--query",
        help="SQL query to execute"
    )
    parser.add_argument(
        "--list-schemas",
        action="store_true",
        help="List all schemas in the catalog"
    )
    parser.add_argument(
        "--list-tables",
        action="store_true",
        help="List all tables in the schema"
    )
    parser.add_argument(
        "--describe",
        action="store_true",
        help="Describe table schema"
    )
    parser.add_argument(
        "--count",
        action="store_true",
        help="Get row count for table"
    )
    parser.add_argument(
        "--output",
        help="Output file path for query results (CSV format)"
    )
    
    args = parser.parse_args()
    
    # Initialize Trino client
    client = TrinoQueryClient(
        host=args.host,
        port=args.port,
        user=args.user,
        catalog=args.catalog,
        schema=args.schema,
        password=args.password,
    )
    
    try:
        # List schemas
        if args.list_schemas:
            schemas = client.list_schemas(args.catalog)
            print(f"\nSchemas in catalog '{args.catalog}':")
            for schema in schemas:
                print(f"  - {schema}")
        
        # List tables
        elif args.list_tables:
            if not args.schema:
                print("Error: --schema is required when using --list-tables", file=sys.stderr)
                sys.exit(1)
            tables = client.list_tables(args.catalog, args.schema)
            print(f"\nTables in '{args.catalog}.{args.schema}':")
            for table in tables:
                print(f"  - {table}")
        
        # Describe table
        elif args.describe:
            if not args.schema or not args.table:
                print("Error: --schema and --table are required when using --describe", file=sys.stderr)
                sys.exit(1)
            df = client.describe_table(args.catalog, args.schema, args.table)
            print(f"\nSchema for '{args.catalog}.{args.schema}.{args.table}':")
            print(df.to_string(index=False))
        
        # Get row count
        elif args.count:
            if not args.schema or not args.table:
                print("Error: --schema and --table are required when using --count", file=sys.stderr)
                sys.exit(1)
            count = client.get_table_row_count(args.catalog, args.schema, args.table)
            print(f"\nRow count for '{args.catalog}.{args.schema}.{args.table}': {count:,}")
        
        # Execute query
        elif args.query:
            df = client.execute_query(args.query, args.catalog, args.schema)
            if df is not None:
                print(f"\nQuery Results ({len(df)} rows):")
                print(df.to_string(index=False))
                
                if args.output:
                    df.to_csv(args.output, index=False)
                    print(f"\nResults saved to {args.output}")
        
        else:
            print("Error: Please specify an action (--query, --list-schemas, --list-tables, --describe, or --count)", file=sys.stderr)
            sys.exit(1)
    
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()

