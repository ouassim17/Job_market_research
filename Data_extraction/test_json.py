from selenium_init import *
with open("Data_extraction/Job_schema.json") as f:
    schema = json.load(f)
with open("Data_extraction/test_val.json", "r", encoding="utf-8") as f:
    data = json.load(f)
try:
    validate(data,schema)
    print("Valid JSON")
except ValidationError as e:
    print("Invalid JSON:", e.message)