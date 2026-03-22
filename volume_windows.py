"""
Controle de volume do sistema Windows (pycaw + teclas virtuais).
Usado pelo backend (system.py) e pelo play.py (servidor de volume na máquina do bar).
SaaS: servidor central faz ponte; a execução real fica no play.py.
"""
import platform
import logging

logger = logging.getLogger(__name__)

VK_VOLUME_UP = 0xAF
VK_VOLUME_DOWN = 0xAE


def com_init() -> bool:
    """Inicializa COM na thread atual (obrigatório para pycaw em threads)."""
    if platform.system() != 'Windows':
        return False
    try:
        import ctypes
        ctypes.windll.ole32.CoInitializeEx(None, 2)  # COINIT_APARTMENTTHREADED
        return True
    except Exception:
        return False


def volume_key(vk_code: int) -> bool:
    """Simula tecla de volume no Windows (VK_VOLUME_UP ou VK_VOLUME_DOWN). Retorna True se ok."""
    if platform.system() != 'Windows':
        return False
    try:
        import ctypes
        ctypes.windll.user32.keybd_event(vk_code, 0, 0, 0)
        ctypes.windll.user32.keybd_event(vk_code, 0, 0x0002, 0)  # KEYEVENTF_KEYUP
        return True
    except Exception as e:
        logger.warning("Falha ao simular tecla de volume no Windows: %s", e)
        return False


def volume_key_up() -> bool:
    """Aumenta o volume do sistema (tecla virtual)."""
    return volume_key(VK_VOLUME_UP)


def volume_key_down() -> bool:
    """Diminui o volume do sistema (tecla virtual)."""
    return volume_key(VK_VOLUME_DOWN)


def get_volume_level() -> int | None:
    """Retorna volume atual 0-100 no Windows (requer pycaw). Retorna None se indisponível."""
    if platform.system() != 'Windows':
        return None
    com_init()
    try:
        from pycaw.pycaw import AudioUtilities
        devices = AudioUtilities.GetSpeakers()
        vol = devices.EndpointVolume
        scalar = vol.GetMasterVolumeLevelScalar()
        return max(0, min(100, round(scalar * 100)))
    except Exception as e:
        logger.info("Volume level (pycaw) indisponível: %s", e, exc_info=True)
        return None


def set_volume_level(pct: int) -> bool:
    """Define volume 0-100 no Windows (requer pycaw). Retorna True se ok."""
    if platform.system() != 'Windows':
        return False
    pct = max(0, min(100, int(pct)))
    com_init()
    try:
        from pycaw.pycaw import AudioUtilities
        devices = AudioUtilities.GetSpeakers()
        vol = devices.EndpointVolume
        vol.SetMasterVolumeLevelScalar(pct / 100.0, None)
        return True
    except Exception as e:
        logger.warning("Falha ao definir volume (pycaw): %s", e, exc_info=True)
        return False


def is_available() -> bool:
    """True se o controle de volume está disponível (Windows)."""
    return platform.system() == 'Windows'
