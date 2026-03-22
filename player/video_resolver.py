from __future__ import annotations


def warm_url(fetch_fn, video_id: str):
    """Wrapper para aquecimento de URL."""
    return fetch_fn(video_id)

