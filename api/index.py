from http.server import BaseHTTPRequestHandler
import json
import os
import sys
import traceback
from urllib.parse import parse_qs, urlparse

# Add the src directory to Python path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

try:
    from mcp_server_motherduck.server import execute_query, get_connection
    # Initialize global configuration
    from mcp_server_motherduck import server as mcp_server
    
    # Set up configuration from environment variables
    mcp_server._db_path = os.getenv('DB_PATH', 'md:')
    mcp_server._motherduck_token = os.getenv('MOTHERDUCK_TOKEN') or os.getenv('motherduck_token')
    mcp_server._read_only = os.getenv('READ_ONLY', 'false').lower() == 'true'
    mcp_server._home_dir = os.getenv('HOME_DIR')
    mcp_server._saas_mode = os.getenv('SAAS_MODE', 'false').lower() == 'true'
    
    MCP_AVAILABLE = True
except ImportError as e:
    print(f"MCP server import error: {e}")
    MCP_AVAILABLE = False

class handler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            # Basic health check
            if self.path == '/health':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                
                status = "ok" if MCP_AVAILABLE else "error"
                response = {
                    "status": status,
                    "service": "MotherDuck MCP Server",
                    "version": "0.6.3",
                    "mcp_available": MCP_AVAILABLE
                }
                
                if MCP_AVAILABLE:
                    try:
                        # Test database connection
                        conn = get_connection()
                        response["database_status"] = "connected"
                        if hasattr(mcp_server, '_read_only') and mcp_server._read_only:
                            conn.close()
                    except Exception as e:
                        response["database_status"] = f"connection_error: {str(e)}"
                
                self.wfile.write(json.dumps(response).encode())
                return

            # API info
            if self.path == '/' or self.path == '/api':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                
                response = {
                    "service": "MotherDuck MCP Server API",
                    "version": "0.6.3",
                    "description": "HTTP API wrapper for MotherDuck MCP Server",
                    "endpoints": {
                        "GET /health": "Health check and status",
                        "GET /": "This API information",
                        "POST /query": "Execute SQL query",
                        "GET /tools": "List available MCP tools",
                        "GET /prompts": "List available MCP prompts"
                    },
                    "authentication": "Bearer token (if API_BEARER_TOKEN env var is set)",
                    "usage": {
                        "query_example": {
                            "method": "POST",
                            "url": "/query",
                            "headers": {
                                "Content-Type": "application/json",
                                "Authorization": "Bearer YOUR_TOKEN"
                            },
                            "body": {
                                "query": "SELECT 1 as test"
                            }
                        }
                    },
                    "mcp_available": MCP_AVAILABLE
                }
                self.wfile.write(json.dumps(response, indent=2).encode())
                return

            # List MCP tools
            if self.path == '/tools':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                
                if MCP_AVAILABLE:
                    tools = [{
                        "name": "query",
                        "description": "Execute a SQL query on the DuckDB or MotherDuck database",
                        "inputSchema": {
                            "type": "object",
                            "properties": {
                                "query": {
                                    "type": "string",
                                    "description": "The SQL query to execute"
                                }
                            },
                            "required": ["query"]
                        }
                    }]
                else:
                    tools = []
                
                response = {
                    "tools": tools,
                    "mcp_available": MCP_AVAILABLE
                }
                self.wfile.write(json.dumps(response).encode())
                return

            # List MCP prompts
            if self.path == '/prompts':
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.send_header('Access-Control-Allow-Origin', '*')
                self.end_headers()
                
                if MCP_AVAILABLE:
                    prompts = [{
                        "name": "duckdb-motherduck-initial-prompt",
                        "description": "A prompt to initialize a connection to DuckDB or MotherDuck and start working with it",
                        "arguments": []
                    }]
                else:
                    prompts = []
                
                response = {
                    "prompts": prompts,
                    "mcp_available": MCP_AVAILABLE
                }
                self.wfile.write(json.dumps(response).encode())
                return

            # Default response
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Endpoint not found"}).encode())

        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.send_header('Access-Control-Allow-Origin', '*')
            self.end_headers()
            error_response = {
                "error": "Internal server error",
                "details": str(e),
                "traceback": traceback.format_exc()
            }
            self.wfile.write(json.dumps(error_response).encode())

    def do_OPTIONS(self):
        """Handle CORS preflight requests"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.end_headers()

    def do_POST(self):
        try:
            # Handle CORS
            self.send_header('Access-Control-Allow-Origin', '*')
            
            # Check authentication
            auth_header = self.headers.get('Authorization', '')
            token = auth_header.replace('Bearer ', '').strip()
            expected_token = os.getenv('API_BEARER_TOKEN')
            
            if expected_token and (not token or token != expected_token):
                self.send_response(401)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps({"error": "Unauthorized - Bearer token required"}).encode())
                return

            # Handle query endpoint
            if self.path == '/query':
                if not MCP_AVAILABLE:
                    self.send_response(500)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({
                        "error": "MCP server not available",
                        "details": "Failed to import MCP server modules"
                    }).encode())
                    return
                
                content_length = int(self.headers['Content-Length'])
                post_data = self.rfile.read(content_length)
                
                try:
                    request_data = json.loads(post_data.decode('utf-8'))
                    query = request_data.get('query', '').strip()
                    
                    if not query:
                        self.send_response(400)
                        self.send_header('Content-type', 'application/json')
                        self.end_headers()
                        self.wfile.write(json.dumps({"error": "Query parameter required"}).encode())
                        return

                    # Execute query using MCP server function
                    result = execute_query(query)
                    
                    self.send_response(200)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps(result, default=str).encode())
                    
                except json.JSONDecodeError:
                    self.send_response(400)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    self.wfile.write(json.dumps({"error": "Invalid JSON in request body"}).encode())
                    
                except Exception as e:
                    self.send_response(500)
                    self.send_header('Content-type', 'application/json')
                    self.end_headers()
                    error_response = {
                        "error": "Query execution failed",
                        "details": str(e),
                        "traceback": traceback.format_exc()
                    }
                    self.wfile.write(json.dumps(error_response).encode())
                return

            # Default POST response
            self.send_response(404)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps({"error": "Endpoint not found"}).encode())

        except Exception as e:
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            error_response = {
                "error": "Internal server error",
                "details": str(e),
                "traceback": traceback.format_exc()
            }
            self.wfile.write(json.dumps(error_response).encode())