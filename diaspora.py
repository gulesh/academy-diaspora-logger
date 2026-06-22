"""Diaspora management CLI: setup credentials, fetch context, and clear topics.

Invoked as: python -m your_pkg.diaspora {setup|context|clear} ...
"""

from __future__ import annotations

import argparse
import json
import sys
import time

from .diaspora_context import get_diaspora_events


def cmd_setup(_args: argparse.Namespace) -> int:
    """Create a Diaspora user (registers credentials via Globus Auth)."""
    from diaspora_event_sdk import Client

    client = Client()
    result = client.create_user()
    print(json.dumps(result, indent=2, default=str))
    return 0


def cmd_context(args: argparse.Namespace) -> int:
    """Fetch recent events from a Diaspora topic."""
    time_horizon = args.time_horizon or int((time.time() - args.lookback) * 1000)

    context = get_diaspora_events(
        topic_name=args.topic,
        time_horizon=time_horizon,
        timeout_ms=args.timeout_ms,
        max_messages=args.max_messages,
    )
    print(json.dumps(context, indent=2, default=str))
    return 0


def cmd_clear(args: argparse.Namespace) -> int:
    """Recreate (clear) a Diaspora topic."""
    from diaspora_event_sdk import Client

    client = Client()
    result = client.recreate_topic(args.topic)
    print(json.dumps(result, indent=2, default=str))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Diaspora management CLI.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("setup", help="Create a Diaspora user (Globus Auth).")

    ctx = subparsers.add_parser("context", help="Fetch recent events from a Diaspora topic.")
    ctx.add_argument("--topic", required=True, help="Diaspora topic name.")
    ctx.add_argument(
        "--time-horizon",
        type=int,
        default=None,
        help="Unix-epoch millisecond timestamp to start from. Overrides --lookback.",
    )
    ctx.add_argument(
        "--lookback",
        type=float,
        default=3600,
        help="Seconds to look back from now (default: 3600). Ignored if --time-horizon is set.",
    )
    ctx.add_argument("--timeout-ms", type=int, default=30000, help="Consumer timeout in ms (default: 30000).")
    ctx.add_argument("--max-messages", type=int, default=10000, help="Max events to fetch (default: 10000).")

    clr = subparsers.add_parser("clear", help="Recreate (clear) a Diaspora topic.")
    clr.add_argument("topic", help="Topic name to recreate.")

    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    handlers = {
        "setup": cmd_setup,
        "context": cmd_context,
        "clear": cmd_clear,
    }
    try:
        return handlers[args.command](args)
    except Exception as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())