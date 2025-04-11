import mcp_pandas.server as mcp_server
import pytest
import base64
import os

@pytest.mark.asyncio
async def test_main():
    """Test the main function."""
    # Test the main function with a simple function
    assert await mcp_server.handle_list_prompts() is not None
    assert await mcp_server.handle_list_tools() is not None
    assert await mcp_server.handle_list_resources() is not None


@pytest.mark.asyncio
@pytest.mark.parametrize("data_path", [
    "src/tests/data/Statements.xlsx",
])
async def test_plot_generation(data_path):
    """Test basic plot generation."""
    # Test the server with a simple function
    mcp_server.load_data(data_path)
    data = await mcp_server.handle_call_tool("plot", {"kind": "bar"})
    assert data is not None
    assert data[1].type == "image"
    # Decode the base64 image
    image_data = data[1].data
    assert image_data is not None
    base64_bytes = base64.b64decode(image_data)
    # Write to png file
    with open("src/tests/test_plot.png", "wb") as f:
        f.write(base64_bytes)
    # Check if the file is created
    assert os.path.exists("src/tests/test_plot.png")


    avg = await mcp_server.handle_call_tool("average", {"column": "Total amount"})
    assert avg is not None
    assert avg[0].text == "Average of Total amount: -0.23"
