#!/usr/bin/env python3

import sys
import os

try:
    # pip install PyYAML to be able to import this
    import yaml
except ImportError:
    print("PyYAML is not installed. Please install it using 'pip install PyYAML'.")
    sys.exit(1)

# Check if the input file is provided
if len(sys.argv) < 2:
    print(f"Usage: {sys.argv[0]} <input-file>")
    sys.exit(1)

input_file = sys.argv[1]
input_dir = os.path.dirname(input_file)
output_file = os.path.join(input_dir, f"sorted_{os.path.basename(input_file)}")

# Read the input YAML file
with open(input_file, 'r') as f:
    documents = list(yaml.safe_load_all(f))

# Sort the documents based on kind and metadata.name
sorted_documents = sorted(documents, key=lambda doc: (
    doc.get('kind', ''), doc.get('metadata', {}).get('name', '')))

# Write the sorted documents to the output file
with open(output_file, 'w') as f:
    yaml.dump_all(sorted_documents, f)

print(f"Sorted YAML written to {output_file}")
