from __future__ import annotations

import threading


def start_heartbeat(loop_fn):
    """Inicia loop de heartbeat em thread daemon."""
    t = threading.Thread(target=loop_fn, daemon=True)
    t.start()
    return t

