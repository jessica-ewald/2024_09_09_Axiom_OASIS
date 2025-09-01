#!/bin/bash

uv venv axiom-env --python=python3.11
source axiom-env/bin/activate
uv pip install -r requirements.txt
python -m ipykernel install --user --name=axiom-env --display-name "Python (axiom-env)"

echo "To activate later: source axiom-env/bin/activate"