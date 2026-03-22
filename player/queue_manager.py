from __future__ import annotations

import threading


def start_queue_monitor(monitor_new_videos_fn, player):
    """Inicia thread de monitoramento da fila."""
    t = threading.Thread(target=monitor_new_videos_fn, args=(player,), daemon=True)
    t.start()
    return t

