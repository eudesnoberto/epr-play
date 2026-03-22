# Player MPV para play.py (máquinas dos bares). Só carregado quando [Player] player_engine=mpv.
# Requer: mpv no PATH (ou [Player] mpv_path no config) e pip install python-mpv
import time

import mpv

from player_common import PlayerState


class MPVPlayer:
    """Wrapper para reprodução com MPV. Interface unificada com VLCPlayer."""

    def __init__(self):
        self._mpv = mpv.MPV(
            input_default_bindings=False,
            input_vo_keyboard=False,
            vo='gpu',
            keep_open='no',
            network_timeout=30,
        )
        self.media_player = self._mpv  # compatibilidade com código que acessa player.media_player
        self.should_skip = False
        self._hidden = True

    def play_video(self, url, video_id=None, audio_url=None, start_position_seconds=None):
        try:
            print(f"🎬 Reproduzindo vídeo (MPV): {url[:100]}...")
            start_sec = int(start_position_seconds) if (start_position_seconds is not None and start_position_seconds > 0) else 0
            # Retomada (mesma regra do VLC): carregar já na posição com loadfile start=+N
            if start_sec > 0:
                try:
                    self._mpv.command('loadfile', url, 'replace', f'start=+{start_sec}')
                    print(f"▶️ Retomada em {start_sec}s")
                except Exception as e:
                    print(f"⚠️ MPV loadfile+start falhou ({e}), tentando play + seek...")
                    self._mpv.play(url)
                    self._apply_resume_position(start_sec)
            else:
                self._mpv.play(url)
            if audio_url:
                try:
                    time.sleep(0.3)
                    self._mpv.command('audio-add', audio_url)
                except Exception as e:
                    print(f"⚠️ MPV: áudio secundário ignorado: {e}")
            if not self._hidden:
                self._mpv.fullscreen = True
        except Exception as e:
            print(f"❌ Erro ao reproduzir vídeo com MPV: {e}")

    def _apply_resume_position(self, start_sec):
        """Aplica posição de retomada quando duration já está disponível (fallback)."""
        for _ in range(60):
            time.sleep(0.25)
            if self.get_length_ms() > 0:
                try:
                    self._mpv.time_pos = start_sec
                    print(f"▶️ Retomada em {start_sec}s")
                except Exception as e:
                    print(f"⚠️ Não foi possível aplicar posição de retomada: {e}")
                return

    def show_player(self):
        self._hidden = False
        try:
            self._mpv.fullscreen = True
        except Exception as e:
            print(f"Não foi possível forçar fullscreen: {e}")

    def hide_player(self):
        self._hidden = True
        try:
            self._mpv.fullscreen = False
            self._mpv.stop()
        except Exception as e:
            print(f"Erro ao ocultar player: {e}")

    def skip_current(self):
        self.should_skip = True
        try:
            self._mpv.stop()
        except Exception:
            pass

    def stop(self):
        try:
            self._mpv.stop()
        except Exception:
            pass

    def pause(self):
        try:
            self._mpv.pause = True
        except Exception:
            pass

    def resume(self):
        try:
            self._mpv.pause = False
        except Exception:
            pass

    def get_state(self):
        try:
            if getattr(self._mpv, 'idle_active', True):
                return PlayerState.ENDED
            if getattr(self._mpv, 'pause', False):
                return PlayerState.PAUSED
            return PlayerState.PLAYING
        except Exception:
            return PlayerState.NONE

    def has_error(self):
        try:
            return False
        except Exception:
            return False

    def is_active(self):
        try:
            if getattr(self._mpv, 'idle_active', True):
                return False
            return True
        except Exception:
            return False

    def get_length_ms(self):
        try:
            d = getattr(self._mpv, 'duration', None)
            if d is not None and d > 0:
                return int(d * 1000)
            return -1
        except Exception:
            return -1

    def get_duration_seconds(self):
        length = self.get_length_ms()
        return length // 1000 if length and length > 0 else 0

    def get_position_seconds(self):
        try:
            pos = getattr(self._mpv, 'time_pos', None)
            if pos is not None and pos >= 0:
                return int(pos)
            return 0
        except Exception:
            return 0
