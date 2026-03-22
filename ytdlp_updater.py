"""
Atualizador automático do yt-dlp (standalone, sem dependências do app Flask).

Usado por:
- play.py (jukebox no bar): verificação em background ao iniciar.
- backend SaaS: via app.dependency_manager (agendamento / fora de horário crítico).

Segurança: apenas executa `pip install -U yt-dlp` (sem execução arbitrária).
Lock de arquivo evita múltiplas atualizações simultâneas (race condition).
"""
from __future__ import annotations

import logging
import os
import subprocess
import sys
import threading
import time
from pathlib import Path

import json
import urllib.request

logger = logging.getLogger(__name__)

# Lock em memória para evitar duas threads no mesmo processo
_update_lock = threading.Lock()

# Nome do lock file (evita dois processos atualizando ao mesmo tempo)
DEFAULT_LOCK_FILE = "ytdlp_update.lock"
LAST_UPDATE_FILE = "ytdlp_last_update.txt"
DEFAULT_LOCK_TIMEOUT_SECONDS = 300  # 5 min max por tentativa
DEFAULT_UPDATE_TIMEOUT_SECONDS = 120  # pip install -U pode demorar
DEFAULT_PYPI_JSON = "https://pypi.org/pypi/yt-dlp/json"


def _get_project_root() -> Path:
    """Raiz do backend (onde está run.py e config.ini)."""
    for name in ("run.py", "config.ini", "play.py"):
        p = Path(__file__).resolve().parent
        if (p / name).exists():
            return p
        if (p.parent / name).exists():
            return p.parent
    return Path(__file__).resolve().parent


def get_installed_ytdlp_version() -> str | None:
    """
    Retorna a versão instalada do yt-dlp (ex: '2023.11.16').
    Usa `python -m yt_dlp --version` para refletir o ambiente atual.
    """
    try:
        out = subprocess.run(
            [sys.executable, "-m", "yt_dlp", "--version"],
            capture_output=True,
            text=True,
            timeout=10,
            env=_safe_env(),
        )
        if out.returncode == 0 and out.stdout:
            return out.stdout.strip()
    except subprocess.TimeoutExpired:
        logger.warning("[yt-dlp] Timeout ao obter versão instalada")
    except Exception as e:
        logger.debug("[yt-dlp] Erro ao obter versão instalada: %s", e)
    return None


def get_latest_ytdlp_version_from_pypi(timeout_seconds: int = 15) -> str | None:
    """
    Obtém a versão mais recente do yt-dlp no PyPI (ex: '2024.1.1').
    """
    url = os.environ.get("YTDLP_PYPI_JSON", DEFAULT_PYPI_JSON)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "ytdlp-updater/1.0"})
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            data = json.loads(resp.read().decode("utf-8"))
            return data.get("info", {}).get("version")
    except Exception as e:
        logger.warning("[yt-dlp] Falha ao obter versão no PyPI: %s", e)
    return None


def _version_tuple(v: str) -> tuple:
    """Converte string de versão em tupla de inteiros para comparação."""
    parts = []
    for x in (v or "").strip().replace("-", ".").split("."):
        try:
            parts.append(int(x))
        except ValueError:
            parts.append(0)
    return tuple(parts) if parts else (0,)


def is_installed_outdated(installed: str | None, latest: str | None) -> bool:
    """True se latest existe e é maior que installed."""
    if not latest:
        return False
    if not installed:
        return True
    return _version_tuple(latest) > _version_tuple(installed)


def _safe_env() -> dict:
    """Ambiente para subprocess: desativa verificação SSL se necessário (Windows)."""
    env = os.environ.copy()
    env.setdefault("PYTHONHTTPSVERIFY", "0")
    env.setdefault("SSL_CERT_FILE", "")
    env.setdefault("REQUESTS_CA_BUNDLE", "")
    env.setdefault("CURL_CA_BUNDLE", "")
    return env


def update_ytdlp(
    timeout_seconds: int = DEFAULT_UPDATE_TIMEOUT_SECONDS,
    retries: int = 2,
) -> tuple[bool, str]:
    """
    Executa `pip install -U yt-dlp` e verifica se a versão subiu.

    Returns:
        (success, message)
    """
    installed_before = get_installed_ytdlp_version()
    for attempt in range(max(1, retries)):
        try:
            logger.info("[yt-dlp] Executando pip install -U yt-dlp (tentativa %s/%s)", attempt + 1, retries)
            result = subprocess.run(
                [sys.executable, "-m", "pip", "install", "-U", "yt-dlp"],
                capture_output=True,
                text=True,
                timeout=timeout_seconds,
                env=_safe_env(),
            )
            if result.returncode != 0:
                stderr = (result.stderr or result.stdout or "")[:500]
                logger.warning("[yt-dlp] pip install falhou: %s", stderr)
                if attempt < retries - 1:
                    time.sleep(2 ** (attempt + 1))
                continue
            installed_after = get_installed_ytdlp_version()
            logger.info(
                "[yt-dlp] Atualização concluída: antes=%s depois=%s",
                installed_before,
                installed_after,
            )
            return True, installed_after or "ok"
        except subprocess.TimeoutExpired:
            logger.warning("[yt-dlp] Timeout na atualização (tentativa %s)", attempt + 1)
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
        except Exception as e:
            logger.warning("[yt-dlp] Erro na atualização: %s", e)
            if attempt < retries - 1:
                time.sleep(2 ** (attempt + 1))
    return False, "falha após retries"


def _config_root(config_dir: Path | str | None) -> Path:
    """Normaliza config_dir para Path (raiz do backend)."""
    if config_dir is not None:
        return Path(config_dir)
    return _get_project_root()


def _lock_file_path(config_dir: Path | str | None) -> Path:
    return _config_root(config_dir) / DEFAULT_LOCK_FILE


def _last_update_file_path(config_dir: Path | str | None) -> Path:
    return _config_root(config_dir) / LAST_UPDATE_FILE


def _read_last_update_time(config_dir: Path | str | None) -> float | None:
    """Retorna timestamp da última atualização bem-sucedida ou None."""
    path = _last_update_file_path(config_dir)
    if not path.exists():
        return None
    try:
        with open(path) as f:
            return float(f.read().strip())
    except Exception:
        return None


def _write_last_update_time(config_dir: Path | str | None) -> None:
    path = _last_update_file_path(config_dir)
    try:
        with open(path, "w") as f:
            f.write(str(time.time()))
    except Exception:
        pass


def _acquire_file_lock(lock_path: Path, timeout_seconds: int) -> bool:
    """
    Lock de arquivo (cria arquivo vazio). Retorna True se conseguiu o lock.
    Lock é liberado ao fechar o handle (melhor usar context manager).
    """
    try:
        fd = os.open(
            str(lock_path),
            os.O_CREAT | os.O_EXCL | os.O_RDWR,
            0o644,
        )
        os.close(fd)
        return True
    except FileExistsError:
        return False
    except Exception:
        return False


def _release_file_lock(lock_path: Path) -> None:
    try:
        if lock_path.exists():
            lock_path.unlink()
    except Exception:
        pass


def run_ytdlp_update_if_outdated(
    config_dir: Path | None = None,
    update_timeout: int = DEFAULT_UPDATE_TIMEOUT_SECONDS,
    lock_timeout: int = DEFAULT_LOCK_TIMEOUT_SECONDS,
    pypi_timeout: int = 15,
    skip_if_updated_recently_seconds: int = 0,
) -> tuple[bool, str]:
    """
    Verifica se yt-dlp está desatualizado; se estiver, adquire lock e atualiza.

    - config_dir: diretório onde criar lock file (default: raiz do backend).
    - skip_if_updated_recently_seconds: se > 0, não atualiza se o lock file
      foi modificado há menos que isso (evita atualizar várias vezes ao dia).

    Returns:
        (updated_or_ok, message)
    """
    if not _update_lock.acquire(blocking=False):
        return False, "atualização já em andamento (lock em memória)"

    lock_path = _lock_file_path(config_dir)
    try:
        if skip_if_updated_recently_seconds > 0:
            last_ts = _read_last_update_time(config_dir)
            if last_ts is not None and (time.time() - last_ts) < skip_if_updated_recently_seconds:
                logger.info("[yt-dlp] Atualização recente detectada (skip_if_updated_recently), pulando")
                return False, "atualização recente"

        installed = get_installed_ytdlp_version()
        latest = get_latest_ytdlp_version_from_pypi(timeout_seconds=pypi_timeout)
        if not is_installed_outdated(installed, latest):
            logger.info("[yt-dlp] Versão atual: %s (PyPI: %s). Nenhuma atualização necessária.", installed, latest)
            return True, installed or "ok"

        if not _acquire_file_lock(lock_path, lock_timeout):
            logger.warning("[yt-dlp] Outro processo já está atualizando (lock file)")
            return False, "lock file ocupado"

        try:
            ok, msg = update_ytdlp(timeout_seconds=update_timeout)
            if ok:
                _write_last_update_time(config_dir)
            return ok, msg
        finally:
            _release_file_lock(lock_path)
    finally:
        try:
            _update_lock.release()
        except Exception:
            pass


def run_ytdlp_update_in_background(
    config_dir: Path | str | None = None,
    delay_seconds: float = 10.0,
    update_timeout: int = DEFAULT_UPDATE_TIMEOUT_SECONDS,
    skip_if_updated_recently_seconds: int = 3600,
    daemon: bool = True,
) -> threading.Thread | None:
    """
    Dispara uma thread que, após delay_seconds, verifica e atualiza o yt-dlp.

    Não bloqueia a inicialização. Seguro para play.py e backend.
    """
    def _job():
        if delay_seconds > 0:
            time.sleep(delay_seconds)
        try:
            run_ytdlp_update_if_outdated(
                config_dir=config_dir,
                update_timeout=update_timeout,
                skip_if_updated_recently_seconds=skip_if_updated_recently_seconds,
            )
        except Exception as e:
            logger.warning("[yt-dlp] Erro no update em background: %s", e)

    t = threading.Thread(target=_job, daemon=daemon, name="ytdlp-updater")
    t.start()
    logger.info("[yt-dlp] Thread de atualização em background iniciada (delay=%.0fs)", delay_seconds)
    return t
