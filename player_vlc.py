# Player VLC para play.py (máquinas dos bares). Só carregado quando [Player] player_engine=vlc.
import os
import sys
import time

if sys.platform == 'win32':
    os.add_dll_directory(r"C:\Program Files\VideoLAN\VLC")
    os.add_dll_directory(r"C:\Program Files\VideoLAN\VLC\plugins")

import vlc

from player_common import PlayerState

_debug_vlc = os.environ.get('DEBUG', '').strip().lower() in ('1', 'true', 'yes')
VLC_INSTANCE_ARGS = [
    '--intf', 'dummy',
    '--no-video-title-show',
    '--network-caching=1000',
    '--file-caching=1000',
    '--live-caching=1000',
    '--avcodec-hw=any',
    '--drop-late-frames',
    '--skip-frames',
    '--quiet',
]
if sys.platform == 'win32':
    VLC_INSTANCE_ARGS.insert(-1, '--vout=direct3d11')
if _debug_vlc:
    VLC_INSTANCE_ARGS = [a for a in VLC_INSTANCE_ARGS if a != '--quiet']


def _vlc_state_to_player_state(vlc_state):
    if vlc_state is None:
        return PlayerState.NONE
    _map = {
        vlc.State.NothingSpecial: PlayerState.NONE,
        vlc.State.Opening: PlayerState.OPENING,
        vlc.State.Buffering: PlayerState.BUFFERING,
        vlc.State.Playing: PlayerState.PLAYING,
        vlc.State.Paused: PlayerState.PAUSED,
        vlc.State.Stopped: PlayerState.STOPPED,
        vlc.State.Ended: PlayerState.ENDED,
        vlc.State.Error: PlayerState.ERROR,
    }
    return _map.get(vlc_state, PlayerState.NONE)


class VLCPlayer:
    """Wrapper para reprodução com VLC. Interface unificada com MPVPlayer."""

    def __init__(self):
        self.instance = vlc.Instance(VLC_INSTANCE_ARGS)
        self.media_player = self.instance.media_player_new()
        self.should_skip = False
        self._hidden = True

    def play_video(self, url, video_id=None, audio_url=None, start_position_seconds=None):
        try:
            print(f"🎬 Reproduzindo vídeo (VLC): {url[:100]}...")
            media = self.instance.media_new(url)
            if audio_url:
                media.add_option(f'input-slave={audio_url}')
            self.media_player.set_media(media)
            self.media_player.audio_set_volume(100)
            if not self._hidden:
                self.media_player.set_fullscreen(True)
            self.media_player.play()
            if start_position_seconds is not None and start_position_seconds > 0:
                for _ in range(50):
                    time.sleep(0.2)
                    if self.get_length_ms() > 0:
                        try:
                            self.media_player.set_time(int(start_position_seconds * 1000))
                            print(f"▶️ Retomada em {start_position_seconds}s")
                        except Exception as e:
                            print(f"⚠️ Não foi possível aplicar posição de retomada: {e}")
                        break
        except Exception as e:
            print(f"❌ Erro ao reproduzir vídeo com VLC: {e}")

    def show_player(self):
        self._hidden = False
        try:
            self.media_player.set_fullscreen(True)
        except Exception as e:
            print(f"Não foi possível forçar fullscreen: {e}")

    def hide_player(self):
        self._hidden = True
        try:
            self.media_player.set_fullscreen(False)
            self.media_player.stop()
        except Exception as e:
            print(f"Erro ao ocultar player: {e}")

    def skip_current(self):
        self.should_skip = True
        try:
            self.media_player.stop()
        except Exception:
            pass

    def stop(self):
        try:
            self.media_player.stop()
        except Exception:
            pass

    def pause(self):
        try:
            self.media_player.pause()
        except Exception:
            pass

    def resume(self):
        try:
            self.media_player.play()
        except Exception:
            pass

    def get_state(self):
        try:
            return _vlc_state_to_player_state(self.media_player.get_state())
        except Exception:
            return PlayerState.NONE

    def has_error(self):
        try:
            return self.media_player.get_state() == vlc.State.Error
        except Exception:
            return False

    def is_active(self):
        s = self.get_state()
        return s in (PlayerState.PLAYING, PlayerState.BUFFERING, PlayerState.PAUSED, PlayerState.OPENING)

    def get_length_ms(self):
        try:
            return self.media_player.get_length()
        except Exception:
            return -1

    def get_duration_seconds(self):
        length = self.get_length_ms()
        return length // 1000 if length and length > 0 else 0

    def get_position_seconds(self):
        try:
            pos = self.media_player.get_time()
            return pos // 1000 if pos and pos > 0 else 0
        except Exception:
            return 0
