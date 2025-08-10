# QuantTrade/utils/dolt_to_duckdb.py
import os
import subprocess
from pathlib import Path
import duckdb
import pandas as pd

def run_cmd(cmd, cwd=None):
    proc = subprocess.run(cmd, cwd=cwd, shell=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, check=False)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({cmd}):\n{proc.stdout}")
    return proc.stdout

def dolt_dump(repo_root: str, remote: str, out_dir: str, dolt_bin: str = "/usr/local/bin/dolt"):
    """
    Clone (if missing) or 'dolt pull' (if present), then 'dolt dump -r csv'.
    Outputs CSVs under out_dir (created if needed).
    """
    repo_root = Path(repo_root)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    env = {"PATH": f"/usr/local/bin:/usr/bin:/bin:{os.environ.get('PATH','')}"}

    # Clone or pull
    if not (repo_root / "options").exists():
        repo_root.mkdir(parents=True, exist_ok=True)
        print(run_cmd(f'{dolt_bin} clone {remote} options', cwd=repo_root, env=env))
    else:
        print(run_cmd(f'{dolt_bin} pull', cwd=repo_root / "options", env=env))

    # Dump CSVs
    # print(run_cmd(f'{dolt_bin} dump -r csv', cwd=repo_root / "options", env=env))
    run_cmd(f"{dolt_bin} dump -r csv -f", cwd=repo_root / "options")
    merge_csv(
        new_csv=repo_root / "options" / "doltdump" / "option_chain.csv",
        old_csv=RAW_DOLT_DIR / "option_chain.csv",
        pk_cols=["symbol", "date"]  # or whatever keys your data uses
    )


    # Dolt writes to {repo_root}/options/doltdump/*
    src = repo_root / "options" / "doltdump"
    if not src.exists():
        raise FileNotFoundError(f"Dolt dump directory not found: {src}")

    # Return absolute CSV paths we care about
    chain_csv = src / "option_chain.csv"
    vol_csv   = src / "volatility_history.csv"
    if not chain_csv.exists() or not vol_csv.exists():
        raise FileNotFoundError(f"Expected CSVs not found in {src}")

    # Copy to out_dir (optional; you can read directly from src)
    chain_out = out_dir / "option_chain.csv"
    vol_out   = out_dir / "volatility_history.csv"
    chain_out.write_bytes(chain_csv.read_bytes())
    vol_out.write_bytes(vol_csv.read_bytes())
    return str(chain_out), str(vol_out)

def csv_to_parquet(csv_path: str, parquet_path: str, chunksize: int = 500_000):
    """Stream CSV → Parquet to avoid memory spikes."""
    csv_path = Path(csv_path)
    parquet_path = Path(parquet_path)
    parquet_path.parent.mkdir(parents=True, exist_ok=True)

    # If file is small, simple path:
    try:
        df = pd.read_csv(csv_path)
        df.to_parquet(parquet_path, index=False)
        return str(parquet_path)
    except MemoryError:
        pass

    # Chunked write (append)
    first = True
    with pd.read_csv(csv_path, chunksize=chunksize) as reader:
        for chunk in reader:
            if first:
                chunk.to_parquet(parquet_path, index=False)
                first = False
            else:
                # append using pyarrow dataset
                existing = pd.read_parquet(parquet_path)
                combined = pd.concat([existing, chunk], axis=0, ignore_index=True)
                combined.to_parquet(parquet_path, index=False)
    return str(parquet_path)

def load_parquet_to_duckdb(db_path: str, parquet_path: str, table: str, pk_cols=None):
    """
    Idempotent-ish load: create table if needed, then upsert new rows by PK.
    If pk_cols is None → simple append + dedupe(all columns).
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    with duckdb.connect(str(db_path)) as con:
        con.execute("PRAGMA threads=4;")
        # Create staging from parquet
        con.execute(f"CREATE OR REPLACE TABLE __stg AS SELECT * FROM read_parquet('{parquet_path}')")

        if pk_cols:
            # Create target table if not exists with same schema as staging
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} AS
                SELECT * FROM __stg WHERE 1=0;
            """)
            # Upsert by PK
            pk_expr = " AND ".join([f"t.{c}=s.{c}" for c in pk_cols])
            cols = [r[0] for r in con.execute(f"PRAGMA table_info('__stg')").fetchall()]
            col_list = ", ".join(cols)

            con.execute(f"""
                INSERT INTO {table}
                SELECT {col_list} FROM __stg s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {table} t WHERE {pk_expr}
                );
            """)
        else:
            # Append then distinct all columns to remove dupes
            con.execute(f"""
                CREATE TABLE IF NOT EXISTS {table} AS
                SELECT * FROM __stg WHERE 1=0;
            """)
            con.execute(f"INSERT INTO {table} SELECT * FROM __stg;")
            cols = [r[0] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
            col_list = ", ".join(cols)
            con.execute(f"""
                CREATE OR REPLACE TABLE {table} AS
                SELECT DISTINCT {col_list} FROM {table};
            """)
        # Cleanup
        con.execute("DROP TABLE IF EXISTS __stg;")

def merge_csv(new_csv, old_csv, pk_cols):
    df_new = pd.read_csv(new_csv)
    if Path(old_csv).exists():
        df_old = pd.read_csv(old_csv)
        df = pd.concat([df_old, df_new]).drop_duplicates(subset=pk_cols)
    else:
        df = df_new
    df.to_csv(old_csv, index=False)

def run_cmd(cmd, cwd=None, env=None):
    env_vars = os.environ.copy()
    if env:
        env_vars.update(env)
    proc = subprocess.run(cmd, cwd=cwd, shell=True,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                          text=True, check=False, env=env_vars)
    if proc.returncode != 0:
        raise RuntimeError(f"Command failed ({cmd}):\n{proc.stdout}")
    return proc.stdout

# NEW: load CSV -> DuckDB with optional PK upsert
# def load_csv_to_duckdb(db_path: str, csv_path: str, table: str, pk_cols=None, infer_types=True):
#     """
#     Create table if missing, then insert rows from CSV.
#     If pk_cols is provided, performs an UPSERT (insert only new PKs).
#     """
#     db_path = Path(db_path)
#     db_path.parent.mkdir(parents=True, exist_ok=True)
#     csv_path = Path(csv_path)

#     with duckdb.connect(str(db_path)) as con:
#         con.execute("PRAGMA threads=4;")
#         # Read CSV via DuckDB (fast + type inference)
#         read_fn = f"read_csv_auto('{csv_path.as_posix()}')" if infer_types else f"read_csv('{csv_path.as_posix()}')"

#         # Stage
#         con.execute(f"CREATE OR REPLACE TABLE __stg AS SELECT * FROM {read_fn}")

#         # Create target with same schema if missing
#         con.execute(f"CREATE TABLE IF NOT EXISTS {table} AS SELECT * FROM __stg WHERE 1=0")

#         if pk_cols:
#             pk_expr = " AND ".join([f"t.{c}=s.{c}" for c in pk_cols])
#             cols = [r[0] for r in con.execute("PRAGMA table_info('__stg')").fetchall()]
#             col_list = ", ".join([f"s.{c}" for c in cols])

#             con.execute(f"""
#                 INSERT INTO {table}
#                 SELECT {col_list}
#                 FROM __stg s
#                 WHERE NOT EXISTS (SELECT 1 FROM {table} t WHERE {pk_expr});
#             """)
#             # Optional: add constraint if you want DuckDB to enforce uniqueness
#             # con.execute(f"ALTER TABLE {table} ADD CONSTRAINT {table}_pk PRIMARY KEY ({', '.join(pk_cols)})")
#         else:
#             con.execute(f"INSERT INTO {table} SELECT * FROM __stg")
#             # De-dupe full rows if you expect repeats
#             cols = [r[0] for r in con.execute(f"PRAGMA table_info('{table}')").fetchall()]
#             col_list = ", ".join(cols)
#             con.execute(f"CREATE OR REPLACE TABLE {table} AS SELECT DISTINCT {col_list} FROM {table}")

#         con.execute("DROP TABLE IF EXISTS __stg;")

# utils/dolt_to_duckdb.py

# def load_csv_to_duckdb(db_path: str, csv_path: str, table: str, pk_cols=None, infer_types=True):
#     """
#     Create table if missing, then insert rows from CSV.
#     If pk_cols is provided, performs an UPSERT (insert only new PKs).
#     """
#     from pathlib import Path
#     import duckdb, os

#     def qid(name: str) -> str:
#         # double-quote and escape embedded quotes
#         return '"' + str(name).replace('"', '""') + '"'

#     db_path = Path(db_path)
#     db_path.parent.mkdir(parents=True, exist_ok=True)
#     csv_path = Path(csv_path)

#     with duckdb.connect(str(db_path)) as con:
#         con.execute("PRAGMA threads=4;")

#         read_fn = f"read_csv_auto('{csv_path.as_posix()}')" if infer_types else f"read_csv('{csv_path.as_posix()}')"

#         # Stage fresh
#         con.execute(f"DROP TABLE IF EXISTS __stg;")
#         con.execute(f"CREATE TABLE __stg AS SELECT * FROM {read_fn}")

#         # Inspect staged columns (helps spot problematic names like '1.0')
#         cols_info = con.execute("PRAGMA table_info('__stg')").fetchall()
#         staged_cols = [r[0] for r in cols_info]
#         print(f"[DuckDB] __stg columns ({len(staged_cols)}): {staged_cols[:12]}{' ...' if len(staged_cols)>12 else ''}")

#         # Create target table if missing with same schema
#         con.execute(f"CREATE TABLE IF NOT EXISTS {qid(table)} AS SELECT * FROM __stg WHERE 1=0")

#         # Build quoted column list
#         col_list = ", ".join(qid(c) for c in staged_cols)

#         if pk_cols:
#             # Build quoted PK predicate
#             pk_expr = " AND ".join([f"{qid('t')}.{qid(c)}={qid('s')}.{qid(c)}" for c in pk_cols])

#             con.execute(f"""
#                 INSERT INTO {qid(table)}
#                 SELECT {", ".join(f'{qid("s")}.{qid(c)}' for c in staged_cols)}
#                 FROM __stg {qid("s")}
#                 WHERE NOT EXISTS (
#                     SELECT 1 FROM {qid(table)} {qid("t")} WHERE {pk_expr}
#                 );
#             """)
#         else:
#             con.execute(f"INSERT INTO {qid(table)} SELECT {col_list} FROM __stg;")
#             # Optional dedupe
#             con.execute(f"""
#                 CREATE OR REPLACE TABLE {qid(table)} AS
#                 SELECT DISTINCT {col_list} FROM {qid(table)};
#             """)

#         con.execute("DROP TABLE IF EXISTS __stg;")

def load_csv_to_duckdb(db_path: str, csv_path: str, table: str, pk_cols=None, infer_types=True):
    """
    Create table if missing, then insert rows from CSV.
    If pk_cols is provided and present in the CSV, performs an UPSERT (insert only new PKs).
    Otherwise: append and then de-dupe full rows.
    """
    from pathlib import Path
    import duckdb, os

    def qid(name: str) -> str:
        return '"' + str(name).replace('"', '""') + '"'

    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    csv_path = Path(csv_path)

    with duckdb.connect(str(db_path)) as con:
        con.execute("PRAGMA threads=4;")

        read_fn = f"read_csv_auto('{csv_path.as_posix()}')" if infer_types else f"read_csv('{csv_path.as_posix()}')"

        # Stage fresh
        con.execute("DROP TABLE IF EXISTS __stg;")
        con.execute(f"CREATE TABLE __stg AS SELECT * FROM {read_fn}")

        # Inspect staged columns
        cols_info = con.execute("PRAGMA table_info('__stg')").fetchall()
        staged_cols = [r[0] for r in cols_info]
        print(f"[DuckDB] __stg columns ({len(staged_cols)}): {staged_cols[:15]}{' ...' if len(staged_cols)>15 else ''}")

        # Create target with same schema if missing
        con.execute(f"CREATE TABLE IF NOT EXISTS {qid(table)} AS SELECT * FROM __stg WHERE 1=0")

        # Quoted column lists
        select_s_cols = ", ".join(f"s.{qid(c)}" for c in staged_cols)
        target_cols   = ", ".join(qid(c) for c in staged_cols)

        do_upsert = bool(pk_cols) and all(pk in staged_cols for pk in pk_cols)

        if do_upsert:
            pk_pred = " AND ".join(f"t.{qid(pk)} = s.{qid(pk)}" for pk in pk_cols)

            # Insert only new PKs
            con.execute(f"""
                INSERT INTO {qid(table)} ({target_cols})
                SELECT {select_s_cols}
                FROM __stg AS s
                WHERE NOT EXISTS (
                    SELECT 1 FROM {qid(table)} AS t
                    WHERE {pk_pred}
                );
            """)
        else:
            # Append then distinct on all columns
            con.execute(f"INSERT INTO {qid(table)} SELECT * FROM __stg;")
            con.execute(f"""
                CREATE OR REPLACE TABLE {qid(table)} AS
                SELECT DISTINCT * FROM {qid(table)};
            """)

        con.execute("DROP TABLE IF EXISTS __stg;")
