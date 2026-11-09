import duckdb

conn = duckdb.connect('controller.duckdb')

# Find exactly one event that contains the voice variant 'V1' so we can see its true JSON structure
result = conn.execute("""
    SELECT event_data 
    FROM raw_events 
    WHERE event_data LIKE '%V1%'
    LIMIT 1
""").fetchone()

if result:
    print(result[0])
else:
    print("No events found containing 'V1'.")