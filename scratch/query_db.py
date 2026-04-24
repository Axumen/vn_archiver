import sqlite3

def query_artifact_types():
    conn = sqlite3.connect('archive.db')
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT COUNT(*) as cnt FROM title;")
        print(f"Titles: {cursor.fetchone()['cnt']}")
        cursor.execute("SELECT COUNT(*) as cnt FROM release;")
        print(f"Releases: {cursor.fetchone()['cnt']}")
        cursor.execute("SELECT COUNT(*) as cnt FROM release_file;")
        print(f"Release files: {cursor.fetchone()['cnt']}")
        
        cursor.execute("SELECT DISTINCT artifact_type FROM release_file;")
        rows = cursor.fetchall()
        print("Existing artifact_type values in database:")
        for row in rows:
            val = row['artifact_type']
            print(f"- '{val}'" if val is not None else "- NULL")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    query_artifact_types()
