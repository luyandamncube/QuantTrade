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

# --- generic Dolt dump for an arbitrary repo name (e.g., "rates") ---
def dolt_dump_repo(repo_root: str, remote: str, repo_name: str, out_dir: str,
                   dolt_bin: str = "/usr/local/bin/dolt") -> dict:
    """
    Clone (if missing) or 'dolt pull' (if present) for repo_name, then 'dolt dump -r csv -f'.
    Copies all CSVs from {repo_root}/{repo_name}/doltdump to out_dir and returns a dict:
      {table_name_without_ext: absolute_csv_path}
    """
    repo_root = Path(repo_root)
    out_dir   = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    env = {"PATH": f"/usr/local/bin:/usr/bin:/bin:{os.environ.get('PATH','')}"}

    if not (repo_root / repo_name).exists():
        repo_root.mkdir(parents=True, exist_ok=True)
        print(run_cmd(f'{dolt_bin} clone {remote} {repo_name}', cwd=repo_root, env=env))
    else:
        print(run_cmd(f'{dolt_bin} pull', cwd=repo_root / repo_name, env=env))

    print(run_cmd(f'{dolt_bin} dump -r csv -f', cwd=repo_root / repo_name, env=env))

    src = repo_root / repo_name / "doltdump"
    if not src.exists():
        raise FileNotFoundError(f"Dolt dump directory not found: {src}")

    mapping = {}
    for p in src.glob("*.csv"):
        table = p.stem  # file name without .csv
        dest  = out_dir / p.name
        dest.write_bytes(p.read_bytes())
        mapping[table] = str(dest)

    if not mapping:
        raise FileNotFoundError(f"No CSVs found in {src}")
    return mapping


# --- bulk CSV -> DuckDB with per-table PK maps ---
def load_many_csvs_to_duckdb(db_path: str, table_to_csv: dict, pk_map: dict | None = None):
    """
    For each table->csv path, load into DuckDB.
    pk_map is an optional dict {table: [pk1, pk2, ...]} for upsert.
    """
    pk_map = pk_map or {}
    for table, csv_path in table_to_csv.items():
        load_csv_to_duckdb(db_path=db_path,
                           csv_path=csv_path,
                           table=table,
                           pk_cols=pk_map.get(table))
        
# --- generic Dolt dump for an arbitrary repo name (e.g., "rates") ---
def dolt_dump_repo(repo_root: str, remote: str, repo_name: str, out_dir: str,
                   dolt_bin: str = "/usr/local/bin/dolt") -> dict:
    """
    Clone (if missing) or 'dolt pull' (if present) for repo_name, then 'dolt dump -r csv -f'.
    Copies all CSVs from {repo_root}/{repo_name}/doltdump to out_dir and returns a dict:
      {table_name_without_ext: absolute_csv_path}
    """
    from pathlib import Path
    import shutil, os

    repo_root = Path(repo_root)
    out_dir   = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    env = {"PATH": f"/usr/local/bin:/usr/bin:/bin:{os.environ.get('PATH','')}"}

    repo_dir = repo_root / repo_name
    is_repo = (repo_dir / ".dolt").exists()

    # If folder exists but not a Dolt repo, remove it (or move aside) and clone fresh
    if repo_dir.exists() and not is_repo:
        print(f"[dolt_dump_repo] '{repo_dir}' exists but is not a Dolt repo. Recreating...")
        shutil.rmtree(repo_dir)

    if not repo_dir.exists():
        repo_root.mkdir(parents=True, exist_ok=True)
        print(run_cmd(f'{dolt_bin} clone {remote} {repo_name}', cwd=repo_root, env=env))
    else:
        # repo exists and has .dolt
        print(run_cmd(f'{dolt_bin} pull', cwd=repo_dir, env=env))

    print(run_cmd(f'{dolt_bin} dump -r csv -f', cwd=repo_dir, env=env))

    src = repo_dir / "doltdump"
    if not src.exists():
        raise FileNotFoundError(f"Dolt dump directory not found: {src}")

    mapping = {}
    for p in src.glob("*.csv"):
        table = p.stem
        dest  = out_dir / p.name
        dest.write_bytes(p.read_bytes())
        mapping[table] = str(dest)

    if not mapping:
        raise FileNotFoundError(f"No CSVs found in {src}")
    return mapping


def load_many_csvs_to_duckdb(db_path: str, table_to_csv: dict, pk_map: dict | None = None):
    """
    For each table->csv path, load into DuckDB.
    pk_map is optional {table: [pk1, pk2, ...]} for upsert.
    """
    pk_map = pk_map or {}
    for table, csv_path in table_to_csv.items():
        load_csv_to_duckdb(db_path=db_path,
                           csv_path=csv_path,
                           table=table,
                           pk_cols=pk_map.get(table))

