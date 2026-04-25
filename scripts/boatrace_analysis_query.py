#!/usr/bin/env python3

from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Sequence, Set

import duckdb

DEFAULT_DB_PATH = "data/boatrace_pipeline.duckdb"
DEFAULT_LIMIT = 100
MAX_LIMIT = 1000
MAX_MARKDOWN_CELL_CHARS = 500

ALLOWED_RELATION_PREFIXES = ("analysis_",)
BLOCKED_KEYWORDS = {
    "ALTER",
    "ATTACH",
    "CALL",
    "COPY",
    "CREATE",
    "DELETE",
    "DETACH",
    "DROP",
    "EXPORT",
    "IMPORT",
    "INSERT",
    "INSTALL",
    "LOAD",
    "PRAGMA",
    "RESET",
    "SET",
    "SUMMARIZE",
    "TRUNCATE",
    "UPDATE",
    "VACUUM",
}
BLOCKED_FUNCTIONS = {
    "glob",
    "query",
    "query_table",
    "read_blob",
    "read_csv",
    "read_json",
    "read_ndjson",
    "read_parquet",
    "read_text",
    "sqlite_scan",
}


class UnsafeSqlError(ValueError):
    pass


def _mask_literals_and_comments(sql: str) -> str:
    result: List[str] = []
    i = 0
    in_single = False
    in_double = False
    while i < len(sql):
        char = sql[i]
        nxt = sql[i + 1] if i + 1 < len(sql) else ""

        if in_single:
            if char == "'" and nxt == "'":
                result.extend("  ")
                i += 2
                continue
            if char == "'":
                in_single = False
            result.append(" ")
            i += 1
            continue

        if in_double:
            if char == '"':
                in_double = False
            result.append(" ")
            i += 1
            continue

        if char == "-" and nxt == "-":
            while i < len(sql) and sql[i] != "\n":
                result.append(" ")
                i += 1
            continue

        if char == "/" and nxt == "*":
            result.extend("  ")
            i += 2
            while i < len(sql):
                if sql[i] == "*" and i + 1 < len(sql) and sql[i + 1] == "/":
                    result.extend("  ")
                    i += 2
                    break
                result.append(" ")
                i += 1
            continue

        if char == "'":
            in_single = True
            result.append(" ")
            i += 1
            continue

        if char == '"':
            in_double = True
            result.append(" ")
            i += 1
            continue

        result.append(char)
        i += 1
    return "".join(result)


def _normalize_sql(sql: str) -> str:
    sql = sql.strip()
    if not sql:
        raise UnsafeSqlError("SQL が空です")
    masked = _mask_literals_and_comments(sql)
    if ";" in masked.rstrip(";"):
        raise UnsafeSqlError("SQL は単一ステートメントだけ実行できます")
    return sql.rstrip().rstrip(";").strip()


def _extract_cte_names(masked_sql: str) -> Set[str]:
    text = masked_sql.strip()
    if not re.match(r"(?is)^WITH\b", text):
        return set()
    names = {
        match.group(1).lower()
        for match in re.finditer(r"(?is)(?:WITH|,)\s+([A-Za-z_][A-Za-z0-9_]*)\s+AS\s*\(", text)
    }
    return names


def _extract_relations(masked_sql: str) -> List[str]:
    relations: List[str] = []
    for match in re.finditer(
        r"(?is)\b(?:FROM|JOIN)\s+([A-Za-z_][A-Za-z0-9_]*(?:\.[A-Za-z_][A-Za-z0-9_]*)?)",
        masked_sql,
    ):
        relations.append(match.group(1).lower())
    return relations


def validate_safe_select(sql: str) -> str:
    normalized = _normalize_sql(sql)
    masked = _mask_literals_and_comments(normalized)
    compact = masked.strip()

    if not re.match(r"(?is)^(SELECT|WITH)\b", compact):
        raise UnsafeSqlError("SQL は SELECT または WITH で始めてください")

    if re.search(r"(?is)\b(?:FROM|JOIN)\s*['\"]", normalized):
        raise UnsafeSqlError("SQL 分析ではファイルパスや文字列をテーブルとして参照できません")

    tokens = {token.upper() for token in re.findall(r"\b[A-Za-z_][A-Za-z0-9_]*\b", masked)}
    blocked = sorted(tokens & BLOCKED_KEYWORDS)
    if blocked:
        raise UnsafeSqlError(f"許可されていない SQL キーワードです: {', '.join(blocked)}")

    function_names = {
        match.group(1).lower()
        for match in re.finditer(r"\b([A-Za-z_][A-Za-z0-9_]*)\s*\(", masked)
    }
    blocked_functions = sorted(function_names & BLOCKED_FUNCTIONS)
    if blocked_functions:
        raise UnsafeSqlError(f"許可されていない関数です: {', '.join(blocked_functions)}")

    cte_names = _extract_cte_names(masked)
    for relation in _extract_relations(masked):
        relation_name = relation.split(".")[-1]
        if relation_name in cte_names:
            continue
        if not relation_name.startswith(ALLOWED_RELATION_PREFIXES):
            raise UnsafeSqlError(
                "SQL 分析では analysis_* ビューだけ参照できます: "
                f"{relation}"
            )

    return normalized


def _rows_to_dicts(cursor: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    columns = [column[0] for column in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _analysis_schema(conn: duckdb.DuckDBPyConnection) -> List[Dict[str, Any]]:
    rows = _rows_to_dicts(
        conn.execute(
            """
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'main'
              AND table_name LIKE 'analysis_%'
            ORDER BY table_name, ordinal_position
            """
        )
    )
    grouped: Dict[str, List[Dict[str, str]]] = {}
    for row in rows:
        grouped.setdefault(row["table_name"], []).append(
            {"name": row["column_name"], "type": row["data_type"]}
        )
    return [{"view": view, "columns": columns} for view, columns in grouped.items()]


def _format_markdown_cell(value: Any) -> str:
    if value is None:
        return ""
    text = str(value)
    text = re.sub(r"[\r\n]+", " ", text).strip()
    text = text.replace("|", r"\|")
    if len(text) > MAX_MARKDOWN_CELL_CHARS:
        text = text[:MAX_MARKDOWN_CELL_CHARS - 1].rstrip() + "…"
    return text


def _render_markdown(rows: Sequence[Dict[str, Any]], limit: int) -> str:
    if not rows:
        return "該当する行はありません。"
    columns = list(rows[0].keys())
    lines = [
        "| " + " | ".join(columns) + " |",
        "| " + " | ".join(["---"] * len(columns)) + " |",
    ]
    for row in rows[:limit]:
        values = [_format_markdown_cell(row.get(column, "")) for column in columns]
        lines.append("| " + " | ".join(values) + " |")
    return "\n".join(lines)


def execute_query(db_path: Path, sql: str, limit: int) -> Dict[str, Any]:
    safe_sql = validate_safe_select(sql)
    safe_limit = max(1, min(limit, MAX_LIMIT))
    with duckdb.connect(str(db_path), read_only=True) as conn:
        wrapped_sql = f"SELECT * FROM ({safe_sql}) AS safe_query LIMIT ?"
        rows = _rows_to_dicts(conn.execute(wrapped_sql, [safe_limit]))
    return {"success": True, "limit": safe_limit, "sql": safe_sql, "rows": rows}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Safe read-only SQL analysis query runner")
    parser.add_argument("--db-path", default=DEFAULT_DB_PATH)
    parser.add_argument("--format", choices=["json", "markdown"], default="markdown")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("schema", help="Show safe analysis views and columns")

    query_parser = subparsers.add_parser("query", help="Run a safe SELECT/WITH query")
    query_parser.add_argument("--sql", help="SQL to run. If omitted, stdin is used.")
    query_parser.add_argument("--limit", type=int, default=DEFAULT_LIMIT)
    return parser


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    db_path = Path(args.db_path)

    try:
        if args.command == "schema":
            with duckdb.connect(str(db_path), read_only=True) as conn:
                result: Dict[str, Any] = {"success": True, "views": _analysis_schema(conn)}
        else:
            sql = args.sql if args.sql is not None else sys.stdin.read()
            result = execute_query(db_path=db_path, sql=sql, limit=args.limit)
    except (duckdb.Error, UnsafeSqlError, OSError) as exc:
        result = {"success": False, "error": str(exc)}
        if args.format == "json":
            print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        else:
            print(f"安全な分析クエリとして実行できません: {exc}")
        return 2

    if args.format == "json":
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    elif args.command == "schema":
        lines = ["# Safe Analysis Views"]
        for view in result["views"]:
            lines.append(f"\n## {view['view']}")
            for column in view["columns"]:
                lines.append(f"- {column['name']}: {column['type']}")
        print("\n".join(lines))
    else:
        print(_render_markdown(result["rows"], result["limit"]))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
