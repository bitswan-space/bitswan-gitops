from fastapi import APIRouter
from fastapi.responses import HTMLResponse
from fastapi import HTTPException
import os

router = APIRouter(prefix="/docs", tags=["documentation"])


@router.get("/promotion-guide", response_class=HTMLResponse)
async def get_promotion_guide():
    """
    Get the promotion workflow guide.
    
    This guide explains how to use the promotion workflow to deploy automations
    through different stages (dev → staging → production) and how to rollback deployments.
    """
    guide_path = os.path.join(
        os.path.dirname(os.path.dirname(__file__)), "docs", "promotion_guide.md"
    )
    
    try:
        with open(guide_path, "r") as f:
            markdown_content = f.read()
        
        # Convert markdown to HTML
        try:
            import markdown
            html_content = markdown.markdown(markdown_content, extensions=['extra', 'fenced_code', 'tables', 'nl2br'])
        except ImportError:
            # Fallback: wrap markdown in pre tags if markdown library not available
            html_content = f"<pre>{markdown_content}</pre>"
        
        # Wrap in a nice HTML template
        html_template = f"""
<!DOCTYPE html>
<html>
<head>
    <title>Automation Promotion Guide</title>
    <style>
        body {{
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, Oxygen, Ubuntu, Cantarell, sans-serif;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            line-height: 1.6;
            color: #333;
        }}
        h1 {{
            color: #2c3e50;
            border-bottom: 3px solid #3498db;
            padding-bottom: 10px;
        }}
        h2 {{
            color: #34495e;
            margin-top: 30px;
            border-bottom: 2px solid #ecf0f1;
            padding-bottom: 5px;
        }}
        h3 {{
            color: #555;
            margin-top: 20px;
        }}
        code {{
            background-color: #f4f4f4;
            padding: 2px 6px;
            border-radius: 3px;
            font-family: 'Courier New', monospace;
            font-size: 0.9em;
        }}
        pre {{
            background-color: #f8f8f8;
            border: 1px solid #ddd;
            border-radius: 5px;
            padding: 15px;
            overflow-x: auto;
        }}
        pre code {{
            background-color: transparent;
            padding: 0;
        }}
        table {{
            border-collapse: collapse;
            width: 100%;
            margin: 20px 0;
        }}
        th, td {{
            border: 1px solid #ddd;
            padding: 12px;
            text-align: left;
        }}
        th {{
            background-color: #3498db;
            color: white;
        }}
        tr:nth-child(even) {{
            background-color: #f9f9f9;
        }}
        blockquote {{
            border-left: 4px solid #3498db;
            margin: 20px 0;
            padding-left: 20px;
            color: #666;
            font-style: italic;
        }}
        ul, ol {{
            margin: 15px 0;
            padding-left: 40px;
            line-height: 1.8;
        }}
        li {{
            margin: 8px 0;
            display: list-item;
        }}
        ol li {{
            list-style-type: decimal;
        }}
        ul li {{
            list-style-type: disc;
        }}
    </style>
</head>
<body>
    {html_content}
</body>
</html>
"""
        return HTMLResponse(content=html_template)
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Promotion guide not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error reading guide: {str(e)}")

