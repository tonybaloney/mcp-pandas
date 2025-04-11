from . import server
import asyncio
import argparse


def main():
    """Main entry point for the package."""
    parser = argparse.ArgumentParser(description='Pandas MCP Server')
    parser.add_argument('--data-path',
                       help='Path to CSV/JSON/Excel file')
    
    args = parser.parse_args()
    asyncio.run(server.main(args.data_path))


# Optionally expose other important items at package level
__all__ = ["main", "server"]