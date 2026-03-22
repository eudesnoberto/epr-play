from __future__ import annotations

import os


def select_player(config):
    """
    Seleciona engine do player (VLC/MPV) baseado em [Player] player_engine.
    Retorna instância do player e nome da engine.
    """
    engine_raw = config.get("Player", "player_engine", fallback="vlc")
    engine = (engine_raw.split("\n")[0].split("\r")[0].strip().lower() if isinstance(engine_raw, str) else "vlc") or "vlc"
    if engine == "mpv":
        mpv_path_raw = config.get("Player", "mpv_path", fallback="") or os.environ.get("MPV_PATH", "")
        mpv_path = mpv_path_raw.split("\n")[0].split("\r")[0].strip() if isinstance(mpv_path_raw, str) else ""
        if mpv_path:
            os.environ["PATH"] = mpv_path + os.pathsep + os.environ.get("PATH", "")
        try:
            from player_mpv import MPVPlayer
            return MPVPlayer(), "mpv"
        except Exception:
            from player_vlc import VLCPlayer
            return VLCPlayer(), "vlc"
    from player_vlc import VLCPlayer
    return VLCPlayer(), "vlc"

