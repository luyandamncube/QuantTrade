import os, re
import duckdb
from collections import defaultdict

# def _norm_type(dt: str) -> str:
#     dt = dt.upper()
#     if any(k in dt for k in ["INT"]): return "int"
#     if any(k in dt for k in ["DOUBLE", "REAL", "FLOAT", "DECIMAL", "NUMERIC"]): return "float"
#     if "DATE" in dt: return "date"
#     if "TIMESTAMP" in dt: return "timestamp"
#     if any(k in dt for k in ["CHAR", "TEXT", "STRING", "VARCHAR"]): return "string"
#     return "string"

# def _safe_name(schema: str, table: str) -> str:
#     # Mermaid identifiers shouldn’t have dots or hyphens
#     return re.sub(r"[^A-Za-z0-9_]", "_", f"{schema}__{table}")

# def generate_mermaid_erd(db_path: str, out_dir: str, erd_name: str = "erd"):
#     os.makedirs(out_dir, exist_ok=True)
#     out_mmd = os.path.join(out_dir, f"{erd_name}.mmd")

#     with duckdb.connect(db_path) as con:
#         tables = con.execute("""
#             SELECT table_schema, table_name
#             FROM information_schema.tables
#             WHERE table_type='BASE TABLE'
#             ORDER BY table_schema, table_name
#         """).fetchall()

#         cols = con.execute("""
#             SELECT table_schema, table_name, column_name, data_type
#             FROM information_schema.columns
#             ORDER BY table_schema, table_name, ordinal_position
#         """).fetchall()

#         pks = con.execute("""
#             SELECT tc.table_schema, tc.table_name, kcu.column_name
#             FROM information_schema.table_constraints tc
#             JOIN information_schema.key_column_usage kcu
#               ON tc.constraint_name = kcu.constraint_name
#              AND tc.table_schema    = kcu.table_schema
#             WHERE tc.constraint_type = 'PRIMARY KEY'
#         """).fetchall()

#         fks = con.execute("""
#             SELECT
#               src.table_schema AS fk_schema,
#               src.table_name   AS fk_table,
#               src.column_name  AS fk_column,
#               tgt.table_schema AS pk_schema,
#               tgt.table_name   AS pk_table,
#               tgt.column_name  AS pk_column
#             FROM information_schema.referential_constraints rc
#             JOIN information_schema.key_column_usage src
#               ON rc.constraint_name = src.constraint_name
#             JOIN information_schema.key_column_usage tgt
#               ON rc.unique_constraint_name = tgt.constraint_name
#              AND src.ordinal_position = tgt.ordinal_position
#             ORDER BY fk_schema, fk_table, src.ordinal_position
#         """).fetchall()

#     cols_by_table = defaultdict(list)
#     pk_by_table   = defaultdict(set)
#     for s,t,c,dt in cols:
#         cols_by_table[(s,t)].append((c, dt))
#     for s,t,c in pks:
#         pk_by_table[(s,t)].add(c)

#     lines = ["erDiagram"]
#     # Entities
#     for s,t in tables:
#         ident = _safe_name(s,t)
#         lines.append(f"{ident} {{")
#         for c, dt in cols_by_table[(s,t)]:
#             typ = _norm_type(dt)
#             suffix = " PK" if c in pk_by_table[(s,t)] else ""
#             lines.append(f"  {typ} {c}{suffix}")
#         lines.append("}")

#     # Relationships (many-to-one default; adjust if you have cardinality metadata)
#     for fs, ft, fc, ps, pt, pc in fks:
#         a = _safe_name(fs, ft)
#         b = _safe_name(ps, pt)
#         # }o--|| = many-to-one; label shows column mapping
#         lines.append(f'{a} }}o--|| {b} : "{fc}→{pc}"')

#     with open(out_mmd, "w", encoding="utf-8") as f:
#         f.write("\n".join(lines))

#     return out_mmd

def _norm_type(dt: str) -> str:
    dt = dt.upper()
    if "INT" in dt: return "int"
    if any(k in dt for k in ["DOUBLE","REAL","FLOAT","DECIMAL","NUMERIC"]): return "float"
    if "DATE" in dt: return "date"
    if "TIMESTAMP" in dt: return "timestamp"
    if any(k in dt for k in ["CHAR","TEXT","STRING","VARCHAR"]): return "string"
    return "string"

def _safe_name(schema: str, table: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", f"{schema}__{table}")

def generate_mermaid_erd(db_path: str, out_dir: str, erd_name: str) -> str:
    os.makedirs(out_dir, exist_ok=True)
    out_mmd = os.path.join(out_dir, f"{erd_name}.mmd")

    with duckdb.connect(db_path) as con:
        tables = con.execute("""
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_type='BASE TABLE'
            ORDER BY table_schema, table_name
        """).fetchall()

        cols = con.execute("""
            SELECT table_schema, table_name, column_name, data_type
            FROM information_schema.columns
            ORDER BY table_schema, table_name, ordinal_position
        """).fetchall()

        pks = con.execute("""
            SELECT tc.table_schema, tc.table_name, kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
              ON tc.constraint_name = kcu.constraint_name
             AND tc.table_schema    = kcu.table_schema
            WHERE tc.constraint_type = 'PRIMARY KEY'
        """).fetchall()

        fks = con.execute("""
            SELECT
              src.table_schema AS fk_schema,
              src.table_name   AS fk_table,
              src.column_name  AS fk_column,
              tgt.table_schema AS pk_schema,
              tgt.table_name   AS pk_table,
              tgt.column_name  AS pk_column
            FROM information_schema.referential_constraints rc
            JOIN information_schema.key_column_usage src
              ON rc.constraint_name = src.constraint_name
            JOIN information_schema.key_column_usage tgt
              ON rc.unique_constraint_name = tgt.constraint_name
             AND src.ordinal_position = tgt.ordinal_position
            ORDER BY fk_schema, fk_table, src.ordinal_position
        """).fetchall()

    cols_by_table = defaultdict(list)
    pk_by_table   = defaultdict(set)
    for s,t,c,dt in cols:
        cols_by_table[(s,t)].append((c, dt))
    for s,t,c in pks:
        pk_by_table[(s,t)].add(c)

    lines = ["erDiagram"]
    # entities
    for s,t in tables:
        ident = _safe_name(s,t)
        lines.append(f"{ident} {{")
        for c, dt in cols_by_table[(s,t)]:
            typ = _norm_type(dt)
            suffix = " PK" if c in pk_by_table[(s,t)] else ""
            lines.append(f"  {typ} {c}{suffix}")
        lines.append("}")
    # relationships (many-to-one default)
    for fs,ft,fc, ps,pt,pc in fks:
        a = _safe_name(fs, ft)
        b = _safe_name(ps, pt)
        lines.append(f'{a} }}o--|| {b} : "{fc}→{pc}"')

    with open(out_mmd, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))
    return out_mmd
