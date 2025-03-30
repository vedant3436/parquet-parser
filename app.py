import os
import json
import pyarrow.parquet as pq
import pandas as pd
from flask import Flask, request, jsonify
from io import BytesIO  # Add this to handle the file stream

app = Flask(__name__)

# Function to parse the parquet file and return the output as a dictionary
def parse_parquet_file(file):
    try:
        # Read file into a BytesIO stream
        file_stream = BytesIO(file.read())  # Convert file to a BytesIO object
        
        # Open the Parquet file
        print(f"Reading Parquet file...")
        parquet_file = pq.ParquetFile(file_stream)

        # Extract schema
        schema = parquet_file.schema_arrow
        schema_dict = {field.name: str(field.type) for field in schema}

        # Extract metadata
        metadata = parquet_file.metadata
        metadata_dict = {
            "num_row_groups": metadata.num_row_groups,
            "num_rows": metadata.num_rows,
            "num_columns": metadata.num_columns,
            "serialized_size": metadata.serialized_size,
            "created_by": metadata.created_by if metadata.created_by else "Unknown"
        }

        # Extract row group metadata (for the first row group, if any)
        row_group_metadata = {}
        if metadata.num_row_groups > 0:
            rg_metadata = metadata.row_group(0)
            row_group_metadata = {
                "num_rows": rg_metadata.num_rows,
                "total_byte_size": rg_metadata.total_byte_size,
                "columns": {}
            }
            for i in range(rg_metadata.num_columns):
                col_chunk = rg_metadata.column(i)
                stats = col_chunk.statistics
                if stats is not None:  # Check if statistics are available
                    row_group_metadata["columns"][schema[i].name] = {
                        "min": stats.min if stats.has_min_max else None,
                        "max": stats.max if stats.has_min_max else None,
                        "null_count": stats.null_count if stats.has_null_count else None
                    }
                else:
                    # If no stats are available, we can handle it here (e.g., set as None or empty)
                    row_group_metadata["columns"][schema[i].name] = {
                        "min": None,
                        "max": None,
                        "null_count": None
                    }

        # Read the data into a Pandas DataFrame
        table = parquet_file.read()
        df = table.to_pandas()

        # Convert DataFrame to a list of dictionaries (records) for JSON
        data_records = df.to_dict(orient='records')

        # Generate summary statistics
        summary = df.describe().to_dict()

        # Construct the full JSON output
        output = {
            "schema": schema_dict,
            "metadata": metadata_dict,
            "row_group_metadata": row_group_metadata,
            "data": data_records,
            "summary": summary
        }

        return output

    except Exception as e:
        print(f"An error occurred: {str(e)}")
        return {"error": str(e)}

# Route to handle file upload and parsing
@app.route('/upload_parquet', methods=['POST'])
def upload_parquet():
    if 'file' not in request.files:
        return jsonify({"error": "No file part"}), 400
    
    file = request.files['file']
    
    if file.filename == '':
        return jsonify({"error": "No selected file"}), 400
    
    try:
        # Call the function to parse the parquet file
        result = parse_parquet_file(file)
        return jsonify(result), 200
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500

# Main entry point to run the app
if __name__ == "__main__":
    app.run(debug=True)
