from mcp.server.fastmcp import FastMCP

# Create MCP app
mcp = FastMCP("HelloWorld")

# Simple hello-world tool
@mcp.tool()
def hello_world() -> str:
    """Return hello world"""
    return "Hello World 👋"

@mcp.tool()
def add_numbers(a: int, b: int) -> int:
    """Add two numbers"""
    return a + b

# Entry point for VS Code Copilot (stdio)
if __name__ == "__main__":
    mcp.run(transport="stdio")
