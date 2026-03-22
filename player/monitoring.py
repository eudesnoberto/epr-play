from __future__ import annotations

import threading


def start_monitoring_threads(
    redis_subscriber_fn,
    keyboard_fn,
    monitor_server_fn,
    player,
):
    """Inicia threads auxiliares de monitoramento."""
    t_redis = threading.Thread(target=redis_subscriber_fn, daemon=True)
    t_redis.start()
    t_keyboard = threading.Thread(target=keyboard_fn, args=(player,), daemon=True)
    t_keyboard.start()
    t_server = threading.Thread(target=monitor_server_fn, args=(player,), daemon=True)
    t_server.start()
    return t_redis, t_keyboard, t_server

