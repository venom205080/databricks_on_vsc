import subprocess
import re
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from sql_parse import generate_excel, validated_table_worksheet, add_errors_to_worksheet

# Load environment variables from the .env file
load_dotenv()


def run_pyspark_script():
    
    file_path = os.getenv('FILE_PATH')
    #read file path
    if not file_path:
        print("Environment variable 'PYSPARK_SCRIPT_PATH' is not set.")
        sys.exit(1)
    
    if not os.path.exists(file_path):
        print(f"The file path '{file_path}' does not exist.")
        sys.exit(1)
    
    #read the data from the file    
    with open(file_path, 'r') as file:
    # Read the entire content of the file
        content = file.read()
        
    #content is in string format 
    generate_excel(content)
    
    path = Path(file_path)
    filename = path.name
        
    # Define the path for the new file
    output_file_path = f"output_{filename}"
        
    # Write the content to a new file but before that convert it to byte like object
    with open(output_file_path, "wb") as file:
        file.write(content.encode('utf-8'))
        
    #append "show tables" line at the end of the file
    with open(output_file_path, 'a') as file:
        file.write("""
tables_df = spark.sql("SHOW TABLES")
print("Created tables:")
for row in tables_df.collect():
    print(f"Table created: {row['tableName']}")
    """)
        
    try:
        #using subprocess to run a command inside python script
        result = subprocess.run(
                ["docker", "run", "-v", f"{os.getcwd()}:/app", "bitnami/spark:3.5.0", "spark-submit", f"/app/{output_file_path}"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True
            )
        
    
            
        pattern = re.compile(r"Table created: (\w+)")
        table_names = pattern.findall(result.stdout)    #table_names variable store names of all the table created
        #add the tables created after validation in worksheet
        validated_table_worksheet(table_names)
        
        add_errors_to_worksheet(result.stdout)    
            
        print(result.stdout)
        # print(matching_lines)

        return { "output": result.stdout}
    except Exception as e:
        return {"status": "failure", "message": "PySpark code failed to run.", "error": str(e)}
    



if __name__ == "__main__":
    run_pyspark_script()