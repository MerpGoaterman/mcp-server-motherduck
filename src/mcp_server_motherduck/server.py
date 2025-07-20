"""MotherDuck MCP Server implementation."""
import asyncio
import logging
import os
import sys
from typing import Any, Dict, List, Optional, Sequence
import json
import traceback

import click
import duckdb
import pytz
from tabulate import tabulate

from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.server.sse import sse_server
from mcp.types import (
    Resource,
    Tool,
    Prompt,
    TextContent,
    ImageContent,
    EmbeddedResource,
    LoggingLevel,
)
import mcp.types as types

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("mcp-server-motherduck")

# Global connection
_connection: Optional[duckdb.DuckDBPyConnection] = None
_db_path: str = "md:"
_motherduck_token: Optional[str] = None
_read_only: bool = False
_home_dir: Optional[str] = None
_saas_mode: bool = False

def get_connection() -> duckdb.DuckDBPyConnection:
    """Get or create a DuckDB connection."""
    global _connection, _db_path, _motherduck_token, _read_only, _home_dir, _saas_mode
    
    if _connection is None or _read_only:
        # For read-only mode, create short-lived connections
        try:
            config = {}
            
            if _home_dir:
                config['home_directory'] = _home_dir
            
            if _saas_mode:
                config['enable_external_access'] = False
            
            if _motherduck_token:
                if _db_path == "md:":
                    conn_string = f"md:?motherduck_token={_motherduck_token}"
                else:
                    conn_string = f"{_db_path}?motherduck_token={_motherduck_token}"
            else:
                conn_string = _db_path
            
            if _read_only and not _db_path.startswith("md:"):
                config['access_mode'] = 'READ_ONLY'
            
            connection = duckdb.connect(conn_string, config=config)
            
            if not _read_only:
                _connection = connection
            
            return connection
            
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            raise
    
    return _connection

def execute_query(query: str) -> Dict[str, Any]:
    """Execute a SQL query and return formatted results."""
    try:
        conn = get_connection()
        
        # Execute the query
        result = conn.execute(query)
        
        # Get column names
        columns = [desc[0] for desc in result.description] if result.description else []
        
        # Fetch all rows
        rows = result.fetchall()
        
        # Close connection if in read-only mode
        if _read_only and conn != _connection:
            conn.close()
        
        # Format results
        if rows:
            # Create table using tabulate
            table = tabulate(rows, headers=columns, tablefmt="grid")
            
            return {
                "success": True,
                "query": query,
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
                "table": table
            }
        else:
            return {
                "success": True,
                "query": query,
                "message": "Query executed successfully (no results returned)",
                "columns": columns,
                "rows": [],
                "row_count": 0,
                "table": "No results"
            }
            
    except Exception as e:
        error_msg = str(e)
        logger.error(f"Query execution failed: {error_msg}")
        return {
            "success": False,
            "query": query,
            "error": error_msg,
            "traceback": traceback.format_exc()
        }

def create_server() -> Server:
    """Create and configure the MCP server."""
    server = Server("mcp-server-motherduck")
    
    @server.list_resources()
    async def handle_list_resources() -> list[Resource]:
        """List available resources."""
        return []
    
    @server.read_resource()
    async def handle_read_resource(uri: str) -> str:
        """Read a specific resource."""
        raise ValueError(f"Resource not found: {uri}")
    
    @server.list_tools()
    async def handle_list_tools() -> list[Tool]:
        """List available tools."""
        return [
            Tool(
                name="query",
                description="Execute a SQL query on the DuckDB or MotherDuck database",
                inputSchema={
                    "type": "object",
                    "properties": {
                        "query": {
                            "type": "string",
                            "description": "The SQL query to execute"
                        }
                    },
                    "required": ["query"]
                }
            )
        ]
    
    @server.call_tool()
    async def handle_call_tool(name: str, arguments: dict) -> list[types.TextContent]:
        """Handle tool calls."""
        if name == "query":
            query = arguments.get("query", "")
            if not query:
                return [types.TextContent(
                    type="text",
                    text="Error: Query parameter is required"
                )]
            
            result = execute_query(query)
            
            if result["success"]:
                response_text = f"Query executed successfully!\n\n"
                response_text += f"**Query:** {result['query']}\n"
                response_text += f"**Rows returned:** {result['row_count']}\n\n"
                
                if result.get("table"):
                    response_text += f"**Results:**\n```\n{result['table']}\n```"
                else:
                    response_text += result.get("message", "Query completed successfully")
                    
            else:
                response_text = f"Query failed!\n\n"
                response_text += f"**Query:** {result['query']}\n"
                response_text += f"**Error:** {result['error']}\n"
                
            return [types.TextContent(type="text", text=response_text)]
        else:
            raise ValueError(f"Unknown tool: {name}")
    
    @server.list_prompts()
    async def handle_list_prompts() -> list[Prompt]:
        """List available prompts."""
        return [
            Prompt(
                name="duckdb-motherduck-initial-prompt",
                description="A prompt to initialize a connection to DuckDB or MotherDuck and start working with it",
                arguments=[]
            )
        ]
    
    @server.get_prompt()
    async def handle_get_prompt(name: str, arguments: dict) -> types.GetPromptResult:
        """Get a specific prompt."""
        if name == "duckdb-motherduck-initial-prompt":
            connection_info = ""
            if _motherduck_token:
                connection_info = "Connected to MotherDuck cloud database"
            elif _db_path == ":memory:":
                connection_info = "Connected to in-memory DuckDB database"
            else:
                connection_info = f"Connected to local DuckDB database: {_db_path}"
            
            mode_info = ""
            if _read_only:
                mode_info = " (read-only mode)"
            if _saas_mode:
                mode_info += " (SaaS mode)"
            
            prompt_text = f"""You are now connected to a DuckDB/MotherDuck database.

{connection_info}{mode_info}

You can now execute SQL queries using the 'query' tool. Some things you can do:

**Data Exploration:**
- `SHOW TABLES;` - List all tables
- `DESCRIBE table_name;` - Show table schema
- `SELECT COUNT(*) FROM table_name;` - Count rows

**MotherDuck Specific (if connected to MotherDuck):**
- `SHOW DATABASES;` - List all databases
- `USE database_name;` - Switch to a different database
- Access shared databases and datasets

**DuckDB Features:**
- Query CSV, Parquet, and JSON files directly
- Use advanced analytics functions
- Join data from multiple sources
- Create views and tables

**Examples:**
- Query a CSV file: `SELECT * FROM 'https://example.com/data.csv' LIMIT 10;`
- Aggregate data: `SELECT category, COUNT(*) FROM table_name GROUP BY category;`
- Time series analysis: `SELECT date_trunc('month', date_column) as month, AVG(value) FROM table_name GROUP BY month;`

What would you like to explore first?"""

            return types.GetPromptResult(
                description="Initialize DuckDB/MotherDuck connection and provide guidance",
                messages=[
                    types.PromptMessage(
                        role="user",
                        content=types.TextContent(type="text", text=prompt_text)
                    )
                ]
            )
        else:
            raise ValueError(f"Unknown prompt: {name}")
    
    return server

@click.command()
@click.option("--transport", type=click.Choice(["stdio", "sse", "stream"]), default="stdio", help="Transport type")
@click.option("--port", type=int, default=8000, help="Port to listen on for sse and stream transport mode")
@click.option("--db-path", type=str, default="md:", help="Path to local DuckDB database file or MotherDuck database")
@click.option("--motherduck-token", type=str, help="Access token to use for MotherDuck database connections")
@click.option("--read-only", is_flag=True, help="Flag for connecting to DuckDB or MotherDuck in read-only mode")
@click.option("--home-dir", type=str, help="Home directory for DuckDB")
@click.option("--saas-mode", is_flag=True, help="Flag for connecting to MotherDuck in SaaS mode")
@click.option("--json-response", is_flag=True, help="Enable JSON responses for HTTP stream")
def main(transport: str, port: int, db_path: str, motherduck_token: Optional[str], 
         read_only: bool, home_dir: Optional[str], saas_mode: bool, json_response: bool):
    """MotherDuck MCP Server."""
    global _db_path, _motherduck_token, _read_only, _home_dir, _saas_mode
    
    # Set global configuration
    _db_path = db_path
    _motherduck_token = motherduck_token or os.getenv("motherduck_token")
    _read_only = read_only
    _home_dir = home_dir or os.getenv("HOME")
    _saas_mode = saas_mode
    
    # Test connection
    try:
        conn = get_connection()
        logger.info("Successfully connected to database")
        if not _read_only:
            # Keep connection open for non-read-only mode
            pass
        else:
            conn.close()
    except Exception as e:
        logger.error(f"Failed to connect to database: {e}")
        sys.exit(1)
    
    # Create server
    server = create_server()
    
    # Run server based on transport
    if transport == "stdio":
        asyncio.run(stdio_server(server))
    elif transport == "sse":
        logger.info(f"Starting SSE server on port {port}")
        asyncio.run(sse_server(server, port=port))
    elif transport == "stream":
        logger.info(f"Starting stream server on port {port}")
        # For stream mode, we'd need additional implementation
        # For now, fall back to SSE
        asyncio.run(sse_server(server, port=port))
    else:
        raise ValueError(f"Unknown transport: {transport}")

if __name__ == "__main__":
    main()