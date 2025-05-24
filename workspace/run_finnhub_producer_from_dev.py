import sys
import os

# Add the parent directory of 'producer' to PYTHONPATH
project_root = os.path.abspath(os.path.dirname(__file__))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from test.main_consumer_test import main

if __name__ == "__main__":
    main()
