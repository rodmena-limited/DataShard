import os
import re
import shutil
import sys
import tempfile

# Add src to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../src")))

def extract_code_blocks(file_path):
    with open(file_path, 'r') as f:
        content = f.read()

    blocks = []
    if file_path.endswith('.md'):
        # Extract ```python ... ``` blocks
        matches = re.findall(r'```python\n(.*?)\n```', content, re.DOTALL)
        blocks.extend(matches)
    elif file_path.endswith('.rst'):
        lines = content.splitlines()
        in_block = False
        current_block = []
        indent = 0

        i = 0
        while i < len(lines):
            line = lines[i]
            if '.. code-block:: python' in line:
                in_block = True
                i += 1
                # consume empty lines
                while i < len(lines) and not lines[i].strip():
                    i += 1
                if i < len(lines):
                    # Determine indentation
                    indent = len(lines[i]) - len(lines[i].lstrip())
                    current_block.append(lines[i][indent:])
            elif in_block:
                if not line.strip():
                    current_block.append("")
                else:
                    current_indent = len(line) - len(line.lstrip())
                    if current_indent < indent:
                        in_block = False
                        if current_block:
                            blocks.append("\n".join(current_block))
                        current_block = []
                        # Re-process this line as it might be start of something else
                        continue
                    else:
                        current_block.append(line[indent:])
            i += 1
        if current_block:
             blocks.append("\n".join(current_block))

    return blocks

def run_block(block, context, file_name, index, temp_dir):
    print(f"Running block {index} from {file_name}...")

    try:
        # Replace common placeholders
        block_to_run = block.replace("/path/to/your/data/table", os.path.join(temp_dir, "table_readme_1"))
        block_to_run = block_to_run.replace("/path/to/pandas_table", os.path.join(temp_dir, "table_readme_2"))
        block_to_run = block_to_run.replace("/path/to/your/table", os.path.join(temp_dir, "table_readme_3"))
        block_to_run = block_to_run.replace("/path/to/table", os.path.join(temp_dir, "table_readme_4"))

        # Quickstart replacements
        block_to_run = block_to_run.replace("/tmp/my_first_table", os.path.join(temp_dir, "table_quick_1"))
        block_to_run = block_to_run.replace("/tmp/complex_table", os.path.join(temp_dir, "table_quick_2"))
        block_to_run = block_to_run.replace("/tmp/time_travel_demo", os.path.join(temp_dir, "table_quick_3"))
        block_to_run = block_to_run.replace("/tmp/workflow_history", os.path.join(temp_dir, "table_quick_4"))

        # Transactions replacements
        block_to_run = block_to_run.replace("/path/to/table", os.path.join(temp_dir, "table_tx_1"))
        block_to_run = block_to_run.replace("/path/to/employees", os.path.join(temp_dir, "table_tx_2"))
        block_to_run = block_to_run.replace("/path/to/metrics", os.path.join(temp_dir, "table_tx_3"))

        parquet_path = os.path.join(temp_dir, "data.parquet")
        if not os.path.exists(parquet_path):
            try:
                import pandas as pd
                df = pd.DataFrame({'a': [1, 2, 3]})
                df.to_parquet(parquet_path)
            except Exception:
                pass # Ignore if pandas not available or fails

        block_to_run = block_to_run.replace("/path/to/data.parquet", parquet_path)
        block_to_run = block_to_run.replace("/path/to/orders", os.path.join(temp_dir, "table_tx_4"))
        block_to_run = block_to_run.replace("/path/to/logs", os.path.join(temp_dir, "table_tx_5"))
        block_to_run = block_to_run.replace("/tmp/events", os.path.join(temp_dir, "table_tx_6"))

        # Exec
        exec(block_to_run, context)
        print("Success")
        return True
    except Exception as e:
        print(f"Failed: {e}")
        # traceback.print_exc()
        return False

def verify_file(file_path):
    print(f"\nVerifying {file_path}")

    # Set storage type based on file
    original_storage_type = os.environ.get("DATASHARD_STORAGE_TYPE")
    if "s3_storage" in file_path:
        os.environ["DATASHARD_STORAGE_TYPE"] = "s3"
    else:
        os.environ["DATASHARD_STORAGE_TYPE"] = "local"

    blocks = extract_code_blocks(file_path)
    context = {}

    temp_dir = tempfile.mkdtemp()
    try:
        success = True
        for i, block in enumerate(blocks):
            if not run_block(block, context, file_path, i+1, temp_dir):
                success = False
        return success
    finally:
        if os.path.exists(temp_dir):
            shutil.rmtree(temp_dir)
        # Restore env
        if original_storage_type:
            os.environ["DATASHARD_STORAGE_TYPE"] = original_storage_type
        else:
            if "DATASHARD_STORAGE_TYPE" in os.environ:
                del os.environ["DATASHARD_STORAGE_TYPE"]

if __name__ == "__main__":
    # Set S3 env vars for S3 examples
    os.environ["DATASHARD_S3_ENDPOINT"] = "https://s3.rodmena.co.uk"
    os.environ["DATASHARD_S3_ACCESS_KEY"] = "rodmena"
    os.environ["DATASHARD_S3_SECRET_KEY"] = "pleasebeready"
    os.environ["DATASHARD_S3_BUCKET"] = "datashard"
    os.environ["DATASHARD_S3_REGION"] = "us-east-1"

    files = [
        "docs/README.md",
        "docs/quickstart.rst",
        "docs/transactions.rst",
        "docs/s3_storage.rst",
    ]

    for f in files:
        verify_file(f)
