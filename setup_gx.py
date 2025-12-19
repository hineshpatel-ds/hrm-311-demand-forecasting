import great_expectations as gx
import os

# This creates the folder structure manually if the CLI fails
context = gx.get_context()
print(f"Great Expectations folder created at: {os.path.abspath('great_expectations')}")