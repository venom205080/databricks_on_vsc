import sqlparse
import re
import xlsxwriter


create_table_patterns = [
        r'CREATE\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([\w.]+)',  # CREATE TABLE statements
        r'CREATE\s+EXTERNAL\s+TABLE\s+(IF\s+NOT\s+EXISTS\s+)?([\w.]+)',  # CREATE TABLE statements
    ]
create_view_pattern = [
    r'CREATE\s+VIEW\s+(IF\s+NOT\s+EXISTS\s+)?([\w.]+)',  # CREATE VIEW statements
    r'CREATE\s+OR\s+REPLACE\s+TEMP\s+VIEW\s+([\w.]+)',  # CREATE VIEW statements
]
drop_table_patterns = [
        r'DROP\s+TABLE\s+(IF\s+EXISTS\s+)?([\w.]+)',        # DROP TABLE statement
    ]
drop_view_pattern = [
    r'DROP\s+VIEW\s+(IF\s+EXISTS\s+)?([\w.]+)',        # DROP TABLE statement
]

alter_table_pattern= [
        r'ALTER\s+TABLE\s+([\w.]+)'                         # ALTER TABLE statements
    ]

pattern_array = [
    create_table_patterns,
    create_view_pattern,
    drop_table_patterns,
    drop_view_pattern,
    alter_table_pattern,
]

def extract_table_names(sql_code, patterns):
    table_names = set()
    for pattern in patterns:
        regex = re.compile(pattern, re.IGNORECASE)
        matches = regex.findall(sql_code)
        for match in matches:
            if isinstance(match, tuple):
                # The table name is in the second group of the tuple
                table_name = match[1]
            else:
                # Directly extract the table name from match
                table_name = match
            table_names.add(table_name.strip())
            
    return table_names

pattern_array_name = [
    "Created Tables before run",
    "Create Views before run",
    "Dropped Tables",
    "Dropped Views",
    "Altered tables",
]

def generate_excel(sql_code):
    wb = xlsxwriter.Workbook('output_excel.xlsx')
    cell_format = wb.add_format({'bold': True})

    for index, i in enumerate(pattern_array):
        worksheet = wb.add_worksheet(f'{pattern_array_name[index].upper()}')            #adding worksheet name
        worksheet.set_column(0, 0, 30)                          #setting column width
        worksheet.write('A1', pattern_array_name[index].upper(), cell_format)           #adding header
        for idx, val in enumerate(extract_table_names(sql_code, i)):
            worksheet.write(f'A{idx+2}', val)
        
    wb.close()
    
    
    
