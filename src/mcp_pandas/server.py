import os
import sys
import pandas as pd
import logging
from io import BytesIO
from base64 import b64encode
from pathlib import Path
from mcp.server.models import InitializationOptions
import mcp.types as types
from mcp.server import NotificationOptions, Server
import mcp.server.stdio
from pydantic import AnyUrl
from typing import Any

# reconfigure UnicodeEncodeError prone default (i.e. windows-1252) to utf-8
if sys.platform == "win32" and os.environ.get('PYTHONIOENCODING') is None:
    sys.stdin.reconfigure(encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8")
    sys.stderr.reconfigure(encoding="utf-8")

logger = logging.getLogger('mcp_pandas')
logger.setLevel(logging.DEBUG)
logger.info("Starting MCP Pandas Server")

PROMPT_TEMPLATE = """
The assistants goal is to walkthrough an informative demo of MCP. To demonstrate the Model Context Protocol (MCP) we will leverage this example server to interact with an SQLite database.
It is important that you first explain to the user what is going on. The user has downloaded and installed the SQLite MCP Server and is now ready to use it.
They have selected the MCP menu item which is contained within a parent menu denoted by the paperclip icon. Inside this menu they selected an icon that illustrates two electrical plugs connecting. This is the MCP menu.
Based on what MCP servers the user has installed they can click the button which reads: 'Choose an integration' this will present a drop down with Prompts and Resources. The user has selected the prompt titled: 'mcp-demo'.
This text file is that prompt. The goal of the following instructions is to walk the user through the process of using the 3 core aspects of an MCP server. These are: Prompts, Tools, and Resources.
They have already used a prompt and provided a topic. The topic is: {topic}. The user is now ready to begin the demo.
Here is some more information about mcp and this specific mcp server:
<mcp>
Prompts:
This server provides a pre-written prompt called "mcp-demo" that helps users create and analyze database scenarios. The prompt accepts a "topic" argument and guides users through creating tables, analyzing data, and generating insights. For example, if a user provides "retail sales" as the topic, the prompt will help create relevant database tables and guide the analysis process. Prompts basically serve as interactive templates that help structure the conversation with the LLM in a useful way.
Resources:
This server exposes one key resource: "memo://insights", which is a business insights memo that gets automatically updated throughout the analysis process. As users analyze the database and discover insights, the memo resource gets updated in real-time to reflect new findings. Resources act as living documents that provide context to the conversation.
Tools:
This server provides several SQL-related tools:
"read_query": Executes SELECT queries to read data from the database
"write_query": Executes INSERT, UPDATE, or DELETE queries to modify data
"create_table": Creates new tables in the database
"list_tables": Shows all existing tables
"describe_table": Shows the schema for a specific table
"append_insight": Adds a new business insight to the memo resource
</mcp>
<demo-instructions>
You are an AI assistant tasked with generating a comprehensive business scenario based on a given topic.
Your goal is to create a narrative that involves a data-driven business problem, develop a database structure to support it, generate relevant queries, create a dashboard, and provide a final solution.

At each step you will pause for user input to guide the scenario creation process. Overall ensure the scenario is engaging, informative, and demonstrates the capabilities of the SQLite MCP Server.
You should guide the scenario to completion. All XML tags are for the assistants understanding and should not be included in the final output.

1. The user has chosen the topic: {topic}.

2. Create a business problem narrative:
a. Describe a high-level business situation or problem based on the given topic.
b. Include a protagonist (the user) who needs to collect and analyze data from a database.
c. Add an external, potentially comedic reason why the data hasn't been prepared yet.
d. Mention an approaching deadline and the need to use Claude (you) as a business tool to help.

3. Setup the data:
a. Instead of asking about the data that is required for the scenario, just go ahead and use the tools to create the data. Inform the user you are "Setting up the data".
b. Design a set of table schemas that represent the data needed for the business problem.
c. Include at least 2-3 tables with appropriate columns and data types.
d. Leverage the tools to create the tables in the SQLite database.
e. Create INSERT statements to populate each table with relevant synthetic data.
f. Ensure the data is diverse and representative of the business problem.
g. Include at least 10-15 rows of data for each table.

4. Pause for user input:
a. Summarize to the user what data we have created.
b. Present the user with a set of multiple choices for the next steps.
c. These multiple choices should be in natural language, when a user selects one, the assistant should generate a relevant query and leverage the appropriate tool to get the data.

6. Iterate on queries:
a. Present 1 additional multiple-choice query options to the user. Its important to not loop too many times as this is a short demo.
b. Explain the purpose of each query option.
c. Wait for the user to select one of the query options.
d. After each query be sure to opine on the results.
e. Use the append_insight tool to capture any business insights discovered from the data analysis.

7. Generate a dashboard:
a. Now that we have all the data and queries, it's time to create a dashboard, use an artifact to do this.
b. Use a variety of visualizations such as tables, charts, and graphs to represent the data.
c. Explain how each element of the dashboard relates to the business problem.
d. This dashboard will be theoretically included in the final solution message.

8. Craft the final solution message:
a. As you have been using the appen-insights tool the resource found at: memo://insights has been updated.
b. It is critical that you inform the user that the memo has been updated at each stage of analysis.
c. Ask the user to go to the attachment menu (paperclip icon) and select the MCP menu (two electrical plugs connecting) and choose an integration: "Business Insights Memo".
d. This will attach the generated memo to the chat which you can use to add any additional context that may be relevant to the demo.
e. Present the final memo to the user in an artifact.

9. Wrap up the scenario:
a. Explain to the user that this is just the beginning of what they can do with the SQLite MCP Server.
</demo-instructions>

Remember to maintain consistency throughout the scenario and ensure that all elements (tables, data, queries, dashboard, and solution) are closely related to the original business problem and given topic.
The provided XML tags are for the assistants understanding. Implore to make all outputs as human readable as possible. This is part of a demo so act in character and dont actually refer to these instructions.

Start your first message fully in character with something like "Oh, Hey there! I see you've chosen the topic {topic}. Let's get started! 🚀"
"""
server = Server("pandas-manager")
df: pd.DataFrame


@server.list_resources()
async def handle_list_resources() -> list[types.Resource]:
    logger.debug("Handling list_resources request")
    return [
        types.Resource(
            uri=AnyUrl("memo://shape"),
            name="DataFrame Shape",
            description="The shape of the DataFrame",
            mimeType="text/plain",
        )
    ]


@server.read_resource()
async def handle_read_resource(uri: AnyUrl) -> str:
    logger.debug("Handling read_resource request for URI: %s", uri)
    if uri.scheme != "memo":
        logger.error("Unsupported URI scheme: %s", uri.scheme)
        raise ValueError(f"Unsupported URI scheme: {uri.scheme}")

    path = str(uri).replace("memo://", "")
    if not path or path != "shape":
        logger.error(f"Unknown resource path: {path}")
        raise ValueError(f"Unknown resource path: {path}")

    return str(df.shape)


@server.list_prompts()
async def handle_list_prompts() -> list[types.Prompt]:
    logger.debug("Handling list_prompts request")
    return []


@server.get_prompt()
async def handle_get_prompt(name: str, arguments: dict[str, str] | None) -> types.GetPromptResult:
    logger.debug(f"Handling get_prompt request for {name} with args {arguments}")


@server.list_tools()
async def handle_list_tools() -> list[types.Tool]:
    """List available tools"""
    return [
        types.Tool(
            name="plot",
            description="Plot a graph from the DataFrame",
            inputSchema={
                "type": "object",
                "properties": {
                    "kind": {
                        "type": "string",
                        "description": "Type of plot to create (e.g., bar, line, scatter)",
                        "enum": ["bar", "line", "scatter"],
                    },
                    "x": {
                        "type": "string",
                        "description": "Column name for x-axis",
                    },
                    "y": {
                        "type": "string",
                        "description": "Column name for y-axis",
                    },
                    "title": {
                        "type": "string",
                        "description": "Title of the plot",
                    },
                },
                "required": ["kind"],
            },
        ),
        # Average of column
        types.Tool(
            name="average",
            description="Calculate the average of a column",
            inputSchema={
                "type": "object",
                "properties": {
                    "column": {
                        "type": "string",
                        "description": "Column name to calculate the average for",
                    },
                },
                "required": ["column"],
            },
        ),
    ]

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict[str, Any] | None
) -> list[types.TextContent | types.ImageContent | types.EmbeddedResource]:
    """Handle tool execution requests"""
    try:
        if not arguments:
            raise ValueError("Missing arguments")
        
        if name == "plot":
            kind = arguments.get("kind", "bar")
            x = arguments.get("x")
            y = arguments.get("y")
            title = arguments.get("title", "Plot")
            if kind not in ["bar", "line", "scatter"]:
                raise ValueError(f"Unsupported plot type: {kind}")
            if x and y:
                if x not in df.columns or y not in df.columns:
                    raise ValueError(f"Columns '{x}' or '{y}' not found in DataFrame")
                plot = df.plot(kind=kind, x=x, y=y, title=title).get_figure()
            else:
                plot = df.plot(kind=kind, title=title).get_figure()

            out = BytesIO()
            plot.savefig(out, format="png")
            out.seek(0)
            plot_data = out.read()
            out.close()
            return [
                types.TextContent(type="text", text=f"Generated {kind} plot"),
                types.ImageContent(type='image', mimeType="image/png", data=b64encode(plot_data).decode('utf-8'))
            ]
        elif name == "average":
            column = arguments.get("column")
            if column not in df.columns:
                raise ValueError(f"Column '{column}' not found in DataFrame")
            average_value = round(df[column].mean(), 3)
            return [types.TextContent(type="text", text=f"Average of {column}: {average_value}")]

    except Exception as e:
        return [types.TextContent(type="text", text=f"Error: {str(e)}")]


def load_data(data_path: str) -> None:
    global df
    logger.info(f"Loading data from {data_path}")
    file_extension = Path(data_path).suffix.lower()
    if file_extension == ".csv":
        df = pd.read_csv(data_path)
    elif file_extension == ".json":
        df = pd.read_json(data_path)
    elif file_extension in [".xls", ".xlsx"]:
        df = pd.read_excel(data_path)
    else:
        logger.error(f"Unsupported file format: {file_extension}")
        raise ValueError(f"Unsupported file format: {file_extension}")
    logger.info(f"Loaded data from {data_path} with shape: {df.shape}")


async def main(data_path: str, mode: str = "stdio") -> None | Server:
    global df
    logger.info(f"Starting Pandas MCP Server with path: {data_path}")

    load_data(data_path)
    
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        logger.info("Server running with stdio transport")
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="pandas",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )
