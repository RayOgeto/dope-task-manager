import json
import os

class Table:
    def __init__(self, name, columns, storage_dir='data'):
        self.name = name
        self.columns = columns 
        self.storage_path = os.path.join(storage_dir, f"{name}.json")
        self.rows = []
        self.indexes = {} 
        self.primary_key = None
        
        for col, props in columns.items():
            if props.get('primary_key') or props.get('unique'):
                self.indexes[col] = {}
            if props.get('primary_key'):
                self.primary_key = col

        self._load()

    def _load(self):
        if os.path.exists(self.storage_path):
            with open(self.storage_path, 'r') as f:
                self.rows = json.load(f)
            self._rebuild_indexes()

    def _save(self):
        os.makedirs(os.path.dirname(self.storage_path), exist_ok=True)
        with open(self.storage_path, 'w') as f:
            json.dump(self.rows, f)

    def _rebuild_indexes(self):
        for col in self.indexes:
            self.indexes[col] = {}
            for i, row in enumerate(self.rows):
                val = row.get(col)
                if val is not None:
                    self.indexes[col][val] = i

    def insert(self, row_data):
        
        new_row = {}
        for col, props in self.columns.items():
            val = row_data.get(col)
            
            
            if col in self.indexes:
                if val in self.indexes[col]:
                    raise ValueError(f"Duplicate value for unique/primary key: {col}={val}")
            
            new_row[col] = val
        
        self.rows.append(new_row)
        idx = len(self.rows) - 1
        for col in self.indexes:
            self.indexes[col][new_row[col]] = idx
        
        self._save()
        return 1

    def select(self, columns='*', where=None):
        results = []
        for i, row in enumerate(self.rows):
            if where:
                match = True
                for col, val in where.items():
                    if row.get(col) != val:
                        match = False
                        break
                if not match:
                    continue
            
            if columns == '*':
                results.append(row.copy())
            else:
                results.append({col: row.get(col) for col in columns})
        return results

    def delete(self, where=None):
        count = 0
        new_rows = []
        for row in self.rows:
            match = True
            if where:
                for col, val in where.items():
                    if row.get(col) != val:
                        match = False
                        break
            else:
                match = True
            
            if match:
                count += 1
            else:
                new_rows.append(row)
        
        self.rows = new_rows
        self._rebuild_indexes()
        self._save()
        return count

    def update(self, updates, where=None):
        count = 0
        for i, row in enumerate(self.rows):
            match = True
            if where:
                for col, val in where.items():
                    if row.get(col) != val:
                        match = False
                        break
            if match:
                for col, val in updates.items():
                    
                    if col in self.indexes and val != row.get(col):
                        if val in self.indexes[col]:
                            raise ValueError(f"Duplicate value for unique key: {col}={val}")
                    row[col] = val
                count += 1
        
        if count > 0:
            self._rebuild_indexes()
            self._save()
        return count

import re
import csv
import io

class Database:
    def __init__(self, storage_dir='data'):
        self.storage_dir = storage_dir
        self.tables = {}
        self.meta_path = os.path.join(storage_dir, 'metadata.json')
        self._load_metadata()

    def _load_metadata(self):
        if os.path.exists(self.meta_path):
            with open(self.meta_path, 'r') as f:
                meta = json.load(f)
                for name, columns in meta.items():
                    self.tables[name] = Table(name, columns, self.storage_dir)

    def _save_metadata(self):
        os.makedirs(self.storage_dir, exist_ok=True)
        meta = {name: t.columns for name, t in self.tables.items()}
        with open(self.meta_path, 'w') as f:
            json.dump(meta, f)

    def execute(self, sql):
        sql = sql.strip()
        if not sql: return None
        
        
        create_match = re.match(r"CREATE\s+TABLE\s+(\w+)\s*\((.*)\)", sql, re.IGNORECASE)
        if create_match:
            table_name = create_match.group(1)
            cols_def = create_match.group(2).split(',')
            columns = {}
            for col in cols_def:
                parts = col.strip().split()
                name = parts[0]
                dtype = parts[1]
                props = {'type': dtype}
                if 'PRIMARY' in col.upper() and 'KEY' in col.upper():
                    props['primary_key'] = True
                if 'UNIQUE' in col.upper():
                    props['unique'] = True
                columns[name] = props
            
            self.tables[table_name] = Table(table_name, columns, self.storage_dir)
            self._save_metadata()
            return f"Table {table_name} created."

        
        insert_match = re.match(r"INSERT\s+INTO\s+(\w+)\s*\((.*)\)\s+VALUES\s*\((.*)\)", sql, re.IGNORECASE)
        if insert_match:
            table_name = insert_match.group(1)
            cols = [c.strip() for c in insert_match.group(2).split(',')]
            
           
            vals_str = insert_match.group(3)
            reader = csv.reader([vals_str], quotechar="'", skipinitialspace=True)
            vals = next(reader)
            vals = [v.strip() for v in vals]
            
            
            row_data = {}
            table = self.tables.get(table_name)
            if not table: raise ValueError(f"Table {table_name} not found")
            
            for col, val in zip(cols, vals):
                if table.columns[col]['type'].upper() == 'INT':
                    row_data[col] = int(val)
                else:
                    row_data[col] = val
            
            count = table.insert(row_data)
            return f"{count} row(s) inserted."

        
        select_match = re.match(r"SELECT\s+(.*)\s+FROM\s+(\w+)(?:\s+JOIN\s+(\w+)\s+ON\s+(\w+)\.(\w+)\s*=\s*(\w+)\.(\w+))?(?:\s+WHERE\s+(.*))?", sql, re.IGNORECASE)
        if select_match:
            cols_str = select_match.group(1).strip()
            table1_name = select_match.group(2)
            table2_name = select_match.group(3)
            
            table1 = self.tables.get(table1_name)
            if not table1: raise ValueError(f"Table {table1_name} not found")

            
            where = None
            where_str = select_match.group(8)
            if where_str:
                w_col, w_val = [x.strip() for x in where_str.split('=')]
                w_val = w_val.strip("'").strip('"')
                
                if w_col in table1.columns and table1.columns[w_col]['type'].upper() == 'INT':
                    w_val = int(w_val)
                where = {w_col: w_val}

            if table2_name:
                table2 = self.tables.get(table2_name)
                if not table2: raise ValueError(f"Table {table2_name} not found")
                
                
                t1_col = select_match.group(5)
                t2_col = select_match.group(7)
                
                res1 = table1.select('*')
                res2 = table2.select('*')
                
                joined = []
                for r1 in res1:
                    for r2 in res2:
                        if r1.get(t1_col) == r2.get(t2_col):
                            merged = {f"{table1_name}.{k}": v for k, v in r1.items()}
                            merged.update({f"{table2_name}.{k}": v for k, v in r2.items()})
                            joined.append(merged)
                
                
                if where:
                    
                    joined = [r for r in joined if any(r.get(f"{t}.{w_col}") == where[w_col] or r.get(w_col) == where[w_col] for t in [table1_name, table2_name])]
                
                return joined
            else:
                cols = '*' if cols_str == '*' else [c.strip() for c in cols_str.split(',')]
                return table1.select(cols, where)

        
        update_match = re.match(r"UPDATE\s+(\w+)\s+SET\s+(.*?)(?:\s+WHERE\s+(.*))?$", sql, re.IGNORECASE)
        if update_match:
            table_name = update_match.group(1)
            set_str = update_match.group(2).strip()
            where_str = update_match.group(3).strip() if update_match.group(3) else None
            
            table = self.tables.get(table_name)
            if not table: raise ValueError(f"Table {table_name} not found")
            
            updates = {}
            for pair in set_str.split(','):
                u_col, u_val = [x.strip() for x in pair.split('=')]
                u_val = u_val.strip("'").strip('"')
                if table.columns[u_col]['type'].upper() == 'INT':
                    u_val = int(u_val)
                updates[u_col] = u_val
                
            where = None
            if where_str:
                w_col, w_val = [x.strip() for x in where_str.split('=')]
                w_val = w_val.strip("'").strip('"')
                if w_col in table.columns and table.columns[w_col]['type'].upper() == 'INT':
                    w_val = int(w_val)
                where = {w_col: w_val}
            
            count = table.update(updates, where)
            return f"{count} row(s) updated."

       
        delete_match = re.match(r"DELETE\s+FROM\s+(\w+)(?:\s+WHERE\s+(.*))?", sql, re.IGNORECASE)
        if delete_match:
            table_name = delete_match.group(1)
            where_str = delete_match.group(2)
            
            table = self.tables.get(table_name)
            if not table: raise ValueError(f"Table {table_name} not found")
            
            where = None
            if where_str:
                w_col, w_val = [x.strip() for x in where_str.split('=')]
                w_val = w_val.strip("'").strip('"')
                if w_col in table.columns and table.columns[w_col]['type'].upper() == 'INT':
                    w_val = int(w_val)
                where = {w_col: w_val}
            
            count = table.delete(where)
            return f"{count} row(s) deleted."

        raise ValueError("Invalid SQL syntax or unsupported command")
