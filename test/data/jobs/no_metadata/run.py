import os

os.makedirs("output", exist_ok=True)
with open("output/greeting", "w") as output_file:
    output_file.write("Hello, R3!\n")
