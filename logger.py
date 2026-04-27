"""Centralised logging configuration for VN Archiver.

All infrastructure and domain modules use the ``vn_archiver`` logger
hierarchy.  Importing this module is *not* required — every module simply
does ``logging.getLogger(__name__)`` and this module configures the root
handler once when the application starts.

Usage (once, at startup — e.g. ``tui.py`` or any entry-point)::

    from logger import configure_logging
    configure_logging()          # INFO to console, optional file path

Usage in any other module::

    import logging
    log = logging.getLogger(__name__)
    log.info("Staged archive for upload: %s", path)
"""

import logging
import sys
from pathlib import Path


# ── ANSI colour codes (used only when the stream is a TTY) ──────────────────

_RESET = "\x1b[0m"
_BOLD  = "\x1b[1m"

_LEVEL_COLOURS = {
    logging.DEBUG:    "\x1b[36m",   # cyan
    logging.INFO:     "\x1b[32m",   # green
    logging.WARNING:  "\x1b[33m",   # yellow
    logging.ERROR:    "\x1b[31m",   # red
    logging.CRITICAL: "\x1b[35m",   # magenta
}


class _ColouredFormatter(logging.Formatter):
    """Formatter that prepends a coloured level tag when writing to a TTY."""

    _FMT = "%(message)s"
    _FMT_WITH_LEVEL = "[%(levelname)s] %(message)s"

    def __init__(self, use_colour: bool = True):
        super().__init__()
        self._use_colour = use_colour

    def format(self, record: logging.LogRecord) -> str:  # noqa: A003
        colour = _LEVEL_COLOURS.get(record.levelno, "")
        msg = record.getMessage()
        if record.exc_info:
            msg = f"{msg}\n{self.formatException(record.exc_info)}"

        if self._use_colour and colour:
            level_tag = f"[{_BOLD}{record.levelname}{_RESET}{colour}]"
            return f"{colour}{level_tag} {msg}{_RESET}"
        if record.levelno >= logging.WARNING:
            return f"[{record.levelname}] {msg}"
        return msg


def configure_logging(
    level: int = logging.INFO,
    log_file: str | Path | None = None,
    file_level: int = logging.DEBUG,
) -> None:
    """Configure the root ``vn_archiver`` logger.

    Parameters
    ----------
    level:
        Minimum severity written to *stderr*.  Defaults to ``INFO``.
    log_file:
        Optional path to a rotating log file.  When supplied, all messages
        at ``file_level`` and above are written there in plain text
        (no colour codes).
    file_level:
        Minimum severity written to the log file.  Defaults to ``DEBUG``
        so that verbose detail is available without cluttering the console.
    """
    root = logging.getLogger("vn_archiver")
    root.setLevel(logging.DEBUG)   # handlers filter further

    # ── Console handler ──────────────────────────────────────────────────────
    console = logging.StreamHandler(sys.stderr)
    console.setLevel(level)
    use_colour = hasattr(sys.stderr, "isatty") and sys.stderr.isatty()
    console.setFormatter(_ColouredFormatter(use_colour=use_colour))
    root.addHandler(console)

    # ── Optional file handler ─────────────────────────────────────────────────
    if log_file is not None:
        from logging.handlers import RotatingFileHandler
        fh = RotatingFileHandler(
            log_file,
            maxBytes=5 * 1024 * 1024,   # 5 MB
            backupCount=3,
            encoding="utf-8",
        )
        fh.setLevel(file_level)
        fh.setFormatter(logging.Formatter(
            "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        ))
        root.addHandler(fh)

    root.debug("Logging initialised (console level=%s)", logging.getLevelName(level))
