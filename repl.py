from dopedb.engine import Database
import sys

def main():
    db = Database()
    print("DopeDB REPL - Type 'exit' to quit")
    while True:
        try:
            sql = input("DopeDB> ")
            if sql.lower() in ('exit', 'quit'):
                break
            if not sql.strip():
                continue
            
            result = db.execute(sql)
            if result is not None:
                if isinstance(result, list):
                    if not result:
                        print("Empty set")
                    else:
                        # Print header
                        keys = result[0].keys()
                        print(" | ".join(keys))
                        print("-" * (len(keys) * 15))
                        for row in result:
                            print(" | ".join(str(row.get(k, '')) for k in keys))
                else:
                    print(result)
        except Exception as e:
            print(f"Error: {e}")

if __name__ == "__main__":
    main()
