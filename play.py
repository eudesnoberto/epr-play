# ===================================================================
# CONFIGURACAO SSL GLOBAL CRITICA - DEVE SER A PRIMEIRA COISA!
# IMPORTANTE: Deve vir ANTES de qualquer import, especialmente yt_dlp
# ===================================================================
import ssl
import os

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


# -------------------------------------------------------------------
# play.py - Cliente do player (rockola)
# Responsável por: reproduzir a fila de vídeos em VLC, sincronizar com
# o backend e reagir a teclas (pular/remover).
#
# BASE_URL: se [Player] server_url estiver definido, usa rede local (ex.: http://127.0.0.1:7000);
# caso contrário usa [Server] dev_url/prod_url (ex.: túnel app.epr.app.br).
# ROTAS DO BACKEND UTILIZADAS (BASE_URL):
#   POST /authenticate   - login com access_code (player usa [Player].code)
#   GET  /get_videos     - lista da fila (com uniqueId, ordem do servidor)
#   POST /remove_video   - remove vídeo por unique_id
#   POST /add_video      - adiciona vídeo (ex.: tecla add)
#   POST /update_video_progress - envia progresso (videoId, currentTime, duration, uniqueId)
#   GET  /health         - monitor de servidor vivo (encerra se offline)
#
# FLUXO PRINCIPAL:
#   1. authenticate() -> token em memória
#   2. Loop em monitor_new_videos():
#      - update_playlist() -> GET /get_videos, ajusta lista local por uniqueId/ordem
#      - Se há vídeo: pega URL (yt-dlp), play_video(VLC), envia progresso, ao fim remove
#      - remove_video_from_list() -> POST /remove_video (ao terminar ou ao pular)
#   3. Thread handle_key_press: tecla skip -> skip + remove no servidor; tecla remove -> idem
#   4. Thread monitor_server_status: GET /health; após N falhas consecutivas, os._exit(1).
#      (Uma única falha não encerra — servidor pode reiniciar ex.: reexec do venv em run.py.)
# -------------------------------------------------------------------

# Desabilitar verificacao SSL globalmente ANTES de importar yt-dlp
ssl._create_default_https_context = ssl._create_unverified_context
import os
# VLC: add_dll_directory só em player_vlc.py quando [Player] player_engine=vlc. Aqui não importamos vlc.
# Configurar variaveis de ambiente para desabilitar SSL
os.environ['PYTHONHTTPSVERIFY'] = '0'
os.environ['CURL_CA_BUNDLE'] = ''
os.environ['REQUESTS_CA_BUNDLE'] = ''
os.environ['SSL_CERT_FILE'] = ''

# Agora sim importar yt-dlp e outros modulos
import atexit
import signal
import sys
import yt_dlp
import requests
import threading
import time
import json
import pathlib
from flask import Flask, jsonify, request
from flask_socketio import SocketIO
import keyboard
import configparser

# Linux: a lib `keyboard` só funciona com root (hooks em /dev/input). Testado uma vez em _init_physical_keyboard_listener.
_physical_keyboard_enabled = None


def _init_physical_keyboard_listener() -> bool:
    """
    Retorna True se atalhos físicos (skip/remove/lista) podem usar `keyboard.is_pressed`.
    No Linux sem root, retorna False (sem spam de erro): skip/remover pelo app continua ativo.
    """
    global _physical_keyboard_enabled
    if _physical_keyboard_enabled is not None:
        return _physical_keyboard_enabled
    if (os.environ.get("DISABLE_KEYBOARD_LISTENER") or "").strip().lower() in ("1", "true", "yes"):
        print("[keyboard] DISABLE_KEYBOARD_LISTENER=1 — atalhos físicos desativados.")
        _physical_keyboard_enabled = False
        return False
    if sys.platform.startswith("linux") and os.geteuid() != 0:
        try:
            keyboard.is_pressed("shift")
        except Exception as e:
            _physical_keyboard_enabled = False
            print(
                "[keyboard] Linux: atalhos de teclado físico exigem rodar como root (sudo) ou permissões uinput. "
                f"Desativados: {e}\n"
                "         Skip/remover/lista pelo app (admin) e Socket.IO continuam funcionando."
            )
            return False
    _physical_keyboard_enabled = True
    return True
import re
import secrets
import subprocess
import browser_cookie3
import certifi
import urllib3
from collections import defaultdict
import hashlib
from urllib.parse import urlparse

# Seleção de engine (MPV ou VLC) via [Player] player_engine no config.ini
from player.player_core import select_player
from player_common import PlayerState

# Desabilitar avisos de SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Carregar .env local (backend/.env) para modo SaaS em jukebox
try:
    if load_dotenv is not None:
        _env_path = os.path.join(os.path.dirname(__file__), '.env')
        if os.path.exists(_env_path):
            load_dotenv(_env_path, override=False)
    # Mesma pasta do backend (run.py existe): usar backend local se PLAYER_SERVER_URL não estiver definido
    _backend_dir = os.path.dirname(os.path.abspath(__file__))
    if not (os.environ.get('PLAYER_SERVER_URL') or '').strip():
        if os.path.isfile(os.path.join(_backend_dir, 'run.py')):
            os.environ.setdefault('PLAYER_SERVER_URL', 'http://127.0.0.1:7000')
except Exception:
    pass

BASE_URL = 'http://127.0.0.1:7000'
SERVER_URL = f'{BASE_URL}/get_videos?player=1&limit=200'
REMOVE_VIDEO_URL = f'{BASE_URL}/remove_video'
ADD_VIDEO_URL = f'{BASE_URL}/add_video'
SKIP_REQUESTED_URL = f'{BASE_URL}/api/skip_requested'
ACCESS_CODES_FILE = 'access_codes.json'

# Caminho para certificados
CERT_PATH = certifi.where()

video_play_count = {}
media_dict = {}
playlist = []
auth_token = None
player_api_key = None
# Modal da lista no PC: tecla exibe/oculta
queue_modal_open = False
close_modal_flag = False
current_playing_video = None
next_video_url = None  # URL do próximo vídeo (pré-carregado)
next_audio_url = None  # URL do áudio do próximo vídeo (DASH), ou None
next_preload_unique_id = None  # uniqueId do vídeo para o qual next_video_url foi pré-carregado (garante ordem da fila)
next_preload_expires_at = None  # epoch (s) de validade do preload legado
preload_thread = None  # Thread para pré-carregar o próximo vídeo
# Cache de preloads por uniqueId para reaproveitar stream quando a fila reordena
preloaded_streams_by_unique_id = {}
preloaded_streams_lock = threading.Lock()
# Lock global para mutações de playlist e current_playing_video
# RLock (reentrante) porque algumas funções chamam outras dentro do mesmo lock
_playlist_lock = threading.RLock()
playlist_refresh_event = threading.Event()  # Sinaliza atualização imediata da fila (Socket.IO)
skip_requested_socket_event = threading.Event()  # Skip em tempo real via Socket.IO
queue_fetch_last_ok = True

# =========================
# Progresso quase realtime
# =========================
edge_progress_socketio_client = None
edge_progress_socketio_connected_event = threading.Event()
edge_progress_socketio_lock = threading.Lock()
edge_progress_socketio_thread = None

# ==== Envio inteligente de progresso (reduz carga) ====
last_progress_sent_ts = 0.0  # timestamp (s) do último envio
last_progress_sent_time = -1  # current_time (s, inteiro) do último envio

# ==== Circuit breaker simples para backend (resiliência/offline) ====
# Após N falhas consecutivas, desabilita tentativas por um período e prioriza fallback local.
backend_fail_count = 0
backend_disabled_until = 0  # epoch seconds
BACKEND_FAIL_THRESHOLD = 6
BACKEND_DISABLE_SECONDS = 15

# Rate limit para não poluir logs durante breaker ativo
_cb_last_skip_log_at = 0.0
_CB_SKIP_LOG_EVERY_SEC = 3.0

# Evitar reprocessamento quando fila não mudou
last_server_queue_hash = None
last_server_queue_len = None

# Streams assinados expiram (YouTube). Sempre validar cache antes de usar.
STREAM_CACHE_DEFAULT_TTL_SEC = 300
STREAM_MIN_VALIDITY_SEC = 60
PRELOAD_STREAM_TTL_SEC = 180
PRELOAD_AHEAD_COUNT = 3
FAILED_VIDEO_COOLDOWN_SEC = 300
FAILED_VIDEO_RETENTION_SEC = 600
_MAX_LAST_PLAYED = 500

# Anti-reprocessamento de vídeos inválidos (em memória, sem depender de estado persistente local)
failed_videos = {}
failed_videos_lock = threading.Lock()

# Evita múltiplas resoluções simultâneas para o mesmo video_id
video_locks = defaultdict(threading.Lock)
_stream_resolve_inflight_lock = threading.Lock()
_stream_resolve_inflight = {}

# Estado operacional Redis observado pelo player (reportado no heartbeat HTTP).
_redis_mode_lock = threading.Lock()
_redis_mode_state = "disabled"  # redis | fallback | disabled

# Cache local da fila (substitui fallback Redis no player)
_QUEUE_CACHE_FILE = pathlib.Path(__file__).resolve().parent / "queue_cache.json"
_QUEUE_CACHE_LOCK = threading.RLock()

# Timeouts de rede para reduzir bloqueios no loop principal
REQUEST_TIMEOUT_GET_VIDEOS_SEC = 2
REQUEST_TIMEOUT_GET_VIDEOS_FIRST_LOAD_SEC = 2
REQUEST_TIMEOUT_SKIP_SEC = 2
REQUEST_TIMEOUT_ADD_SEC = 8
QUEUE_FETCH_MAX_RETRIES = 3
queue_first_fetch_pending = True

# GET /health no monitor — HTTPS remoto (ex.: app.epr.app.br) pode demorar >15s (TLS, rota, cold start).
# Subir via [Player] health_check_timeout_seconds ou env HEALTH_CHECK_TIMEOUT_SEC.
HEALTH_CHECK_TIMEOUT_SEC = int(os.environ.get("HEALTH_CHECK_TIMEOUT_SEC", "25"))


def _backend_time_left_disabled_sec() -> int:
    try:
        now = int(time.time())
        until = int(backend_disabled_until or 0)
        return max(0, until - now)
    except Exception:
        return 0


def _backend_can_try() -> bool:
    return _backend_time_left_disabled_sec() <= 0


def _cb_log_skip(left_s: int | None = None):
    """Evita spam de logs quando breaker está ativo."""
    global _cb_last_skip_log_at
    try:
        now = time.time()
        if (now - float(_cb_last_skip_log_at or 0.0)) < float(_CB_SKIP_LOG_EVERY_SEC):
            return
        _cb_last_skip_log_at = now
    except Exception:
        pass
    if left_s is None:
        try:
            left_s = _backend_time_left_disabled_sec()
        except Exception:
            left_s = 0
    print(f"[CIRCUIT_BREAKER] skipping backend request (disabled) left_s={left_s}")


def _backend_record_success():
    global backend_fail_count, backend_disabled_until
    backend_fail_count = 0
    backend_disabled_until = 0


def _backend_record_failure():
    global backend_fail_count, backend_disabled_until
    # Não incrementar falhas enquanto o breaker estiver ativo
    try:
        if not _backend_can_try():
            return
    except Exception:
        pass

    try:
        backend_fail_count = int(backend_fail_count) + 1
    except Exception:
        backend_fail_count = 1

    if backend_fail_count >= BACKEND_FAIL_THRESHOLD:
        backend_disabled_until = int(time.time()) + int(BACKEND_DISABLE_SECONDS)
        # NÃO resetar contador aqui; só resetar em sucesso real do backend
        backend_fail_count = BACKEND_FAIL_THRESHOLD
        print(f"[CIRCUIT_BREAKER] backend disabled for {BACKEND_DISABLE_SECONDS} seconds")

# SocketIO não é necessário no play.py (é apenas cliente HTTP)


def log_event(event_type, data=None):
    """Log estruturado (JSON) para observabilidade em produção."""
    payload = {
        "event": event_type,
        "timestamp": int(time.time() * 1000),
    }
    if isinstance(data, dict):
        payload.update(data)
    try:
        print(json.dumps(payload, ensure_ascii=False))
    except Exception:
        print(f"[EVENT] {event_type} {data or {}}")


def _cleanup_failed_videos_cache():
    now = int(time.time())
    with failed_videos_lock:
        stale = [
            vid for vid, info in failed_videos.items()
            if now - int(info.get("last_failed_at", 0)) > FAILED_VIDEO_RETENTION_SEC
        ]
        for vid in stale:
            failed_videos.pop(vid, None)


def _mark_video_failed(video_id, reason):
    if not video_id:
        return
    now = int(time.time())
    with failed_videos_lock:
        failed_videos[video_id] = {
            "last_failed_at": now,
            "reason": str(reason or "unknown"),
        }


def _get_recent_failure(video_id):
    if not video_id:
        return None
    now = int(time.time())
    with failed_videos_lock:
        info = failed_videos.get(video_id)
        if not info:
            return None
        if now - int(info.get("last_failed_at", 0)) < FAILED_VIDEO_COOLDOWN_SEC:
            return info
        return None


def _clear_video_failure(video_id):
    if not video_id:
        return
    with failed_videos_lock:
        failed_videos.pop(video_id, None)


def _run_volume_server():
    """
    Servidor HTTP mínimo para controle de volume na máquina do bar (SaaS).
    O central faz ponte: app -> central -> proxy para machine_url -> este servidor.
    Rotas: GET/PATCH /api/system_volume, POST /api/system_volume_up, /api/system_volume_down.
    """
    import sys
    import importlib.util
    _backend_dir = os.path.dirname(os.path.abspath(__file__))
    if _backend_dir not in sys.path:
        sys.path.insert(0, _backend_dir)
    # 1) Deploy nos bares: volume_windows.py na MESMA pasta do play.py (import direto)
    volume_windows = None
    first_err = None
    try:
        import volume_windows as vw  # type: ignore
        volume_windows = vw
    except Exception as e:
        first_err = e
    # 2) Deploy com backend completo: pacote app.utils.volume_windows
    if volume_windows is None:
        try:
            from app.utils import volume_windows as vw  # type: ignore
            volume_windows = vw
        except Exception as e2:
            first_err = first_err or e2
    # 3) Fallback: carregar pelo caminho backend/app/utils/volume_windows.py
    if volume_windows is None:
        try:
            vol_path = os.path.join(_backend_dir, "app", "utils", "volume_windows.py")
            spec = importlib.util.spec_from_file_location("volume_windows_local", vol_path)
            if spec and spec.loader:
                vw = importlib.util.module_from_spec(spec)
                spec.loader.exec_module(vw)  # type: ignore[attr-defined]
                volume_windows = vw
        except Exception as e3:
            print(f"[Volume] Módulo volume_windows indisponível (volume na máquina desabilitado): {first_err} / {e3}")
            return
    if volume_windows is None:
        print(f"[Volume] Módulo volume_windows indisponível (volume na máquina desabilitado): {first_err}")
        return
    if not volume_windows.is_available():
        print("[Volume] Controle de volume só disponível no Windows; servidor de volume não iniciado.")
        return
    cfg = load_config()
    # 7001: mesma porta que o central usa para monitor_servers; na máquina do bar só roda play.py.
    port = 7001
    if cfg.has_section('Volume'):
        port = cfg.getint('Volume', 'port', fallback=7001)
    app = Flask(__name__)

    @app.route('/api/system_volume', methods=['GET'])
    def volume_get():
        print("[Volume] GET /api/system_volume")
        level = volume_windows.get_volume_level()
        if level is not None:
            print(f"[Volume] GET /api/system_volume -> 200 (volume={level})")
            return jsonify({'success': True, 'volume': level}), 200
        print("[Volume] GET /api/system_volume -> 200 (nível indisponível)")
        return jsonify({
            'success': True,
            'volume': None,
            'message': 'Nível indisponível; use os botões para alterar o volume.'
        }), 200

    @app.route('/api/system_volume', methods=['PATCH'])
    def volume_set():
        data = request.get_json(silent=True) or {}
        pct = data.get('volume')
        if pct is None:
            print("[Volume] PATCH /api/system_volume -> 400 (volume não enviado)")
            return jsonify({'success': False, 'message': 'Envie "volume" (0-100).'}), 400
        try:
            pct = int(pct)
        except (TypeError, ValueError):
            print("[Volume] PATCH /api/system_volume -> 400 (volume inválido)")
            return jsonify({'success': False, 'message': 'Volume deve ser número.'}), 400
        ok = volume_windows.set_volume_level(pct)
        if not ok:
            print(f"[Volume] PATCH /api/system_volume -> 501 (falha ao definir {pct})")
            return jsonify({'success': False, 'message': 'Não foi possível definir volume. Use os botões.'}), 501
        print(f"[Volume] PATCH /api/system_volume -> 200 (volume={pct})")
        return jsonify({'success': True, 'volume': max(0, min(100, pct))}), 200

    @app.route('/api/system_volume_up', methods=['POST'])
    def volume_up():
        print("[Volume] POST /api/system_volume_up")
        ok = volume_windows.volume_key_up()
        level = volume_windows.get_volume_level() if ok else None
        payload = {'success': ok, 'message': 'Volume aumentado' if ok else 'Falha ao aumentar volume'}
        if level is not None:
            payload['volume'] = level
        print(f"[Volume] POST /api/system_volume_up -> {'200' if ok else '500'} (level={level})")
        return jsonify(payload), 200 if ok else 500

    @app.route('/api/system_volume_down', methods=['POST'])
    def volume_down():
        print("[Volume] POST /api/system_volume_down")
        ok = volume_windows.volume_key_down()
        level = volume_windows.get_volume_level() if ok else None
        payload = {'success': ok, 'message': 'Volume diminuído' if ok else 'Falha ao diminuir volume'}
        if level is not None:
            payload['volume'] = level
        print(f"[Volume] POST /api/system_volume_down -> {'200' if ok else '500'} (level={level})")
        return jsonify(payload), 200 if ok else 500

    try:
        print(f"[Volume] Servidor de volume na máquina escutando em 0.0.0.0:{port} (machine_url = http://<IP_MAQUINA>:{port})")
        app.run(host='0.0.0.0', port=port, threaded=True, use_reloader=False, debug=False)
    except OSError as e:
        if "address already in use" in str(e).lower() or "10048" in str(e):
            print(f"[Volume] ERRO: Porta {port} já em uso. Nesta máquina também roda monitor_servers? Use [Volume] port no config.ini com outra porta.")
        else:
            print(f"[Volume] Erro ao iniciar servidor de volume: {e}")
    except Exception as e:
        print(f"[Volume] Erro ao iniciar servidor de volume: {e}")


def _expand_env_in_value(value):
    """Substitui ${VAR} em value pelos valores de os.environ (para config.ini com placeholders)."""
    if not value or not isinstance(value, str):
        return value or ''
    import re
    def repl(m):
        name = m.group(1)
        return os.environ.get(name, '')
    return re.sub(r'\$\{([^}]+)\}', repl, value).strip()

def load_config():
    config = configparser.ConfigParser()
    config_path = os.path.join(os.path.dirname(__file__), 'config.ini')

    if not os.path.exists(config_path):
        print("Aviso: config.ini não encontrado; usando variáveis de ambiente/defaults")
        return config

    config.read(config_path)
    return config

# URL de exemplo no .env.example; só ignorar quando vier da variável de ambiente (testes locais)
_PLACEHOLDER_SERVER_URL = 'https://app.epr.app.br'


def _redact_url_for_log(raw_url: str) -> str:
    """Oculta credenciais da URL para logs de diagnóstico."""
    try:
        from urllib.parse import urlparse
        u = (raw_url or '').strip()
        if not u:
            return ''
        p = urlparse(u)
        if not p.scheme:
            return u
        host = p.hostname or ''
        port = f":{p.port}" if p.port else ''
        path = p.path or ''
        return f"{p.scheme}://{host}{port}{path}"
    except Exception:
        return "<invalid_url>"

def _set_server_urls(config_obj):
    global BASE_URL, SERVER_URL, REMOVE_VIDEO_URL, ADD_VIDEO_URL, SKIP_REQUESTED_URL
    try:
        env_server_url = (os.environ.get('PLAYER_SERVER_URL') or '').strip()
        # Ignorar só quando a ÚNICA fonte é o .env com placeholder (evita SSL em testes locais)
        if env_server_url.rstrip('/') == _PLACEHOLDER_SERVER_URL.rstrip('/'):
            env_server_url = ''
        # Expandir placeholders ${PLAYER_SERVER_URL} do config.ini
        raw_player_url = config_obj.get('Player', 'server_url', fallback='').strip()
        player_server_url = _expand_env_in_value(raw_player_url) if raw_player_url else ''
        # Ignorar só se ainda tem placeholder não expandido ou valor vazio
        if player_server_url and '${' in player_server_url:
            player_server_url = ''
        # Se server_url no config for https://app.epr.app.br (produção), usar normalmente
        if env_server_url:
            base_url = env_server_url
        elif player_server_url:
            base_url = player_server_url
        else:
            env = config_obj.get('Server', 'environment', fallback='production').lower()
            if env == 'development':
                base_url = config_obj.get('Server', 'dev_url', fallback=None)
            elif env == 'production':
                base_url = config_obj.get('Server', 'prod_url', fallback=None)
            else:
                base_url = config_obj.get('Server', 'url', fallback=None)
            base_url = base_url or config_obj.get('Server', 'default_url', fallback=BASE_URL)
        # Fallback final: mesma máquina (backend em 7000)
        if not base_url or not base_url.startswith(('http://', 'https://')):
            base_url = BASE_URL or 'http://127.0.0.1:7000'
        BASE_URL = base_url.rstrip('/')
    except Exception:
        BASE_URL = BASE_URL.rstrip('/')

    SERVER_URL = f'{BASE_URL}/get_videos?player=1&limit=200'
    REMOVE_VIDEO_URL = f'{BASE_URL}/remove_video'
    ADD_VIDEO_URL = f'{BASE_URL}/add_video'
    SKIP_REQUESTED_URL = f'{BASE_URL}/api/skip_requested'


def _log_player_routing_diagnostics():
    """Mostra, no startup, para qual backend o player está apontando."""
    try:
        sid = (_get_current_server_id() or '').strip() or 'UNKNOWN'
    except Exception:
        sid = 'UNKNOWN'
    print(
        "[BOOT] player routing:"
        f" server_id={sid}"
        f" base_url={_redact_url_for_log(BASE_URL)}"
        " queue_cache=queue_cache.json"
    )

# Carregar configurações
try:
    config = load_config()
    _set_server_urls(config)
    # Teclas e qualidade (uma única definição)
    debounce = 1.0
    if config.has_section('Security'):
        debounce = config.getfloat('Security', 'key_debounce_time', fallback=1.0)
    KEYBOARD_CONFIG = {
        'generate_access_code': config.get('Keyboard', 'generate_access_code', fallback='a'),
        'skip_video': config.get('Keyboard', 'skip_video', fallback='1'),
        'remove_video': config.get('Keyboard', 'remove_video', fallback='5'),
        'show_list': config.get('Keyboard', 'show_list', fallback='l'),
        'debounce_time': debounce,
    }
    VIDEO_QUALITY_CONFIG = {
        # 0 = melhor resolução disponível (sem teto); >0 = limita altura em pixels (ex.: 1080)
        'max_height': config.getint('Video', 'max_height', fallback=0),
    }
    # Configurações do yt-dlp (JS runtime e cookies) — opcional (config do painel pode não ter [YtDlp])
    if config.has_section('YtDlp'):
        YTDLP_CONFIG = {
            'js_runtimes': config.get('YtDlp', 'js_runtimes', fallback='').strip(),
            'cookies_file': config.get('YtDlp', 'cookies_file', fallback='').strip(),
            'cookies_from_browser': config.get('YtDlp', 'cookies_from_browser', fallback='').strip(),
            'cookies_profile': config.get('YtDlp', 'cookies_profile', fallback='').strip(),
            'try_cookies_first': config.getboolean('YtDlp', 'try_cookies_first', fallback=False),
        }
    else:
        YTDLP_CONFIG = {
            'js_runtimes': '',
            'cookies_file': '',
            'cookies_from_browser': '',
            'cookies_profile': '',
            'try_cookies_first': False
        }
    # Timeouts de fila HTTP (ajustáveis no config.ini para filas grandes)
    if config.has_section('Player'):
        REQUEST_TIMEOUT_GET_VIDEOS_SEC = config.getint(
            'Player', 'get_videos_timeout_seconds', fallback=REQUEST_TIMEOUT_GET_VIDEOS_SEC
        )
        REQUEST_TIMEOUT_GET_VIDEOS_FIRST_LOAD_SEC = config.getint(
            'Player',
            'get_videos_first_load_timeout_seconds',
            fallback=config.getint(
                'Player',
                'get_videos_fallback_timeout_seconds',
                fallback=REQUEST_TIMEOUT_GET_VIDEOS_FIRST_LOAD_SEC
            )
        )
        # Respeitar timeouts configurados com limites defensivos.
        # Em redes remotas, limitar em 2s força retries em cascata e piora o tempo percebido de startup.
        try:
            REQUEST_TIMEOUT_GET_VIDEOS_SEC = max(1, min(8, int(REQUEST_TIMEOUT_GET_VIDEOS_SEC)))
        except Exception:
            REQUEST_TIMEOUT_GET_VIDEOS_SEC = 3
        try:
            REQUEST_TIMEOUT_GET_VIDEOS_FIRST_LOAD_SEC = max(2, min(25, int(REQUEST_TIMEOUT_GET_VIDEOS_FIRST_LOAD_SEC)))
        except Exception:
            REQUEST_TIMEOUT_GET_VIDEOS_FIRST_LOAD_SEC = 8
        try:
            BACKEND_FAIL_THRESHOLD = max(
                3,
                min(
                    12,
                    int(config.get('Player', 'backend_fail_threshold', fallback=str(BACKEND_FAIL_THRESHOLD)))
                )
            )
        except Exception:
            BACKEND_FAIL_THRESHOLD = 6
        try:
            BACKEND_DISABLE_SECONDS = max(
                5,
                min(
                    120,
                    int(config.get('Player', 'backend_disable_seconds', fallback=str(BACKEND_DISABLE_SECONDS)))
                )
            )
        except Exception:
            BACKEND_DISABLE_SECONDS = 15
        try:
            HEALTH_CHECK_TIMEOUT_SEC = max(
                5,
                min(
                    120,
                    int(
                        config.get(
                            'Player',
                            'health_check_timeout_seconds',
                            fallback=str(HEALTH_CHECK_TIMEOUT_SEC),
                        )
                    ),
                ),
            )
        except Exception:
            pass
    # Configurações por estabelecimento (créditos, distância, controles) — cada máquina pode ter seu config.ini
    def _get_establishment_config(cfg):
        out = {'minutes_per_real': 8, 'min_distance': 100, 'allow_skip': True, 'allow_remove': True,
               'allow_show_list': True, 'max_users': 7, 'nome': '', 'descricao': '', 'endereco': '', 'telefone': ''}
        try:
            if cfg.has_section('Credits'):
                out['minutes_per_real'] = cfg.getint('Credits', 'minutes_per_real', fallback=8)
            if cfg.has_section('Distance'):
                out['min_distance'] = cfg.getint('Distance', 'min_distance', fallback=100)
            if cfg.has_section('Controles'):
                out['allow_skip'] = cfg.getboolean('Controles', 'allow_skip', fallback=True)
                out['allow_remove'] = cfg.getboolean('Controles', 'allow_remove', fallback=True)
                out['allow_show_list'] = cfg.getboolean('Controles', 'allow_show_list', fallback=True)
                out['max_users'] = cfg.getint('Controles', 'max_users', fallback=7)
            if cfg.has_section('Estabelecimento'):
                out['nome'] = (cfg.get('Estabelecimento', 'nome', fallback='') or '').strip()
                out['descricao'] = (cfg.get('Estabelecimento', 'descricao', fallback='') or '').strip()
                out['endereco'] = (cfg.get('Estabelecimento', 'endereco', fallback='') or '').strip()
                out['telefone'] = (cfg.get('Estabelecimento', 'telefone', fallback='') or '').strip()
        except Exception:
            pass
        return out
    ESTABLISHMENT_CONFIG = _get_establishment_config(config)
except Exception as e:
    print(f"Erro ao carregar configurações: {e}")
    print("Usando configurações padrão")
    config = None
    KEYBOARD_CONFIG = {
        'generate_access_code': 'a',
        'skip_video': '1',
        'remove_video': '5',
        'show_list': 'l',
        'debounce_time': 1.0
    }
    VIDEO_QUALITY_CONFIG = {'max_height': 0}
    YTDLP_CONFIG = {
        'js_runtimes': '',
        'cookies_file': '',
        'cookies_from_browser': '',
        'cookies_profile': '',
        'try_cookies_first': False,
    }
    ESTABLISHMENT_CONFIG = {
        'minutes_per_real': 8,
        'min_distance': 100,
        'allow_skip': True,
        'allow_remove': True,
        'allow_show_list': True,
        'max_users': 7,
        'nome': '',
        'descricao': '',
        'endereco': '',
        'telefone': '',
    }

def _parse_csv(value):
    return [item.strip() for item in value.split(',') if item.strip()]

def _apply_ydl_auth_options(ydl_opts, include_cookie_file=True, include_browser_cookies=True):
    js_runtimes = _parse_csv(YTDLP_CONFIG.get('js_runtimes', ''))
    if js_runtimes:
        # yt-dlp espera um dict: {runtime: {config}}
        ydl_opts['js_runtimes'] = {runtime: {} for runtime in js_runtimes}

    cookies_file = YTDLP_CONFIG.get('cookies_file')
    if include_cookie_file and cookies_file and os.path.exists(cookies_file):
        ydl_opts['cookiefile'] = cookies_file

    if include_browser_cookies:
        browser = YTDLP_CONFIG.get('cookies_from_browser')
        if browser:
            profile = YTDLP_CONFIG.get('cookies_profile')
            if profile:
                ydl_opts['cookiesfrombrowser'] = (browser, profile)
            else:
                ydl_opts['cookiesfrombrowser'] = (browser,)


def _is_cookie_db_missing_error(err_text: str) -> bool:
    s = (err_text or "").lower()
    return (
        "could not find firefox cookies database" in s
        or "could not find chrome cookies database" in s
        or "could not find edge cookies database" in s
        or ("could not find" in s and "cookies database" in s)
    )

# Player é criado via select_player(load_config()) no main (MPV ou VLC conforme [Player] player_engine).

def get_valid_access_code():
    try:
        with open(ACCESS_CODES_FILE, 'r') as f:
            access_codes = json.load(f)
            if access_codes:
                # Pega o primeiro código de acesso válido
                code = next(iter(access_codes.keys()))
                print(f"Código de acesso válido encontrado: {code}")
                return code
    except FileNotFoundError:
        print(f"Arquivo {ACCESS_CODES_FILE} não encontrado (opcional; fallback só com API Key falhando).")
    except Exception as e:
        print(f"Erro ao ler códigos de acesso: {e}")
    return None


def _print_backend_unreachable_hint(status_code, response_text, exc=None):
    """
    Explica quando o backend não responde JSON (túnel Cloudflare desligado, backend offline, etc.).
    """
    text = (response_text or "")[:500].lower()
    is_html = "<!doctype" in text or "<html" in text or text.strip().startswith("<")
    print("")
    print("=" * 60)
    print("  ATENÇÃO — Não foi possível falar com o backend em:", BASE_URL)
    if exc and not status_code:
        print("  Erro de conexão (rede/timeout/recusado): servidor pode estar OFF ou túnel DOWN.")
    if status_code == 530:
        print("  HTTP 530 costuma indicar: túnel (Cloudflare/Zero Trust) DESLIGADO ou origem inacessível.")
    elif status_code in (502, 503, 504):
        print(f"  HTTP {status_code}: gateway/servidor indisponível ou sobrecarga.")
    elif is_html or (status_code and status_code != 200):
        print("  A resposta foi HTML (página de erro), não JSON da API.")
        print("  Isso costuma significar: túnel entre o servidor e a internet OFF, ou URL errada.")
    if exc:
        print(f"  Detalhe: {exc}")
    print("  O que verificar:")
    print("    • Backend (run.py) está rodando no servidor?")
    print("    • Túnel/agente (Cloudflare, ngrok, etc.) está ativo no mesmo host da API?")
    print("    • [Player] server_url no config.ini = URL correta (ex.: https://app.epr.app.br)")
    print("=" * 60)
    print("")

def get_player_access_code():
    """Obtém código do player com prioridade: ENV PLAYER_CODE > config.ini [Player]."""
    try:
        env_code = (os.environ.get('PLAYER_CODE') or '').strip()
        if env_code:
            return env_code
        config_obj = load_config()
        raw_code = config_obj.get('Player', 'code', fallback='').strip()
        code = _expand_env_in_value(raw_code) if raw_code else ''
        if code and '${' in code:
            code = ''
        if code:
            return code
    except Exception as e:
        print(f"Erro ao obter código do player: {e}")
    return None

def get_player_api_key():
    """Obtém API key com prioridade: ENV PLAYER_API_KEY > config.ini [Player]."""
    try:
        env_key = (os.environ.get('PLAYER_API_KEY') or '').strip()
        if env_key:
            return env_key
        config_obj = load_config()
        raw_key = config_obj.get('Player', 'api_key', fallback='').strip()
        key = _expand_env_in_value(raw_key) if raw_key else ''
        # Evita usar placeholder literal (${PLAYER_API_KEY}) como chave real
        if key and '${' in key:
            key = ''
        if key:
            return key
    except Exception as e:
        print(f"Erro ao obter api_key do player: {e}")
    return None


def _get_play_config_path():
    return os.path.join(os.path.dirname(__file__), 'config.ini')


def _needs_provisioning() -> bool:
    """
    Provisionamento é necessário quando não há API key operacional do player.
    Mantém fallback manual por código/API key sem interromper startup.
    """
    return not bool((get_player_api_key() or '').strip())


def _save_api_key_to_config(api_key: str) -> bool:
    """
    Persiste API key no config.ini em [Player] api_key.
    """
    key = (api_key or '').strip()
    if not key:
        return False
    try:
        config_path = _get_play_config_path()
        cfg = configparser.ConfigParser()
        if os.path.exists(config_path):
            cfg.read(config_path, encoding='utf-8')
        if not cfg.has_section('Player'):
            cfg.add_section('Player')
        cfg.set('Player', 'api_key', key)
        with open(config_path, 'w', encoding='utf-8') as f:
            cfg.write(f)
        print(f"[PROVISION] api_key gravada em {config_path}")
        return True
    except Exception as e:
        print(f"[PROVISION] Falha ao gravar api_key no config.ini: {e}")
        return False


def _run_provisioning_bootstrap() -> bool:
    """
    Provisiona máquina automaticamente usando bootstrap token.
    Não bloqueia fallback manual em caso de erro.
    """
    server_id = (_get_current_server_id() or '').strip()
    bootstrap_token = (os.environ.get('MACHINE_BOOTSTRAP_TOKEN') or '').strip()
    if not bootstrap_token:
        print("[PROVISION] MACHINE_BOOTSTRAP_TOKEN não configurado — provisionamento automático desabilitado.")
        return False
    if not server_id or server_id == 'LOCAL':
        print("[PROVISION] server_id inválido/ausente no config.ini — configure [Server] server_id.")
        return False
    if not (BASE_URL or '').strip():
        print("[PROVISION] BASE_URL inválida — não foi possível iniciar bootstrap.")
        return False

    print(f"[PROVISION] Iniciando provisionamento para server_id={server_id}...")
    try:
        resp = requests.post(
            f"{BASE_URL}/api/machines/provision",
            json={
                "server_id": server_id,
                "bootstrap_token": bootstrap_token,
            },
            timeout=10,
            verify=False,
            proxies=REQUESTS_NO_PROXY,
            headers=REQUEST_HEADERS_CONNECTION_CLOSE,
        )
        if resp.status_code not in (200, 201):
            print(f"[PROVISION] Falha no provisionamento: HTTP {resp.status_code}")
            return False
        payload = resp.json() if resp.text else {}
        api_key = (payload.get('api_key') or '').strip() if isinstance(payload, dict) else ''
        if not api_key:
            print("[PROVISION] Resposta sem api_key — provisionamento falhou.")
            return False
        if not _save_api_key_to_config(api_key):
            return False
        provisioned = bool(payload.get('provisioned', True)) if isinstance(payload, dict) else True
        action = "provisionada" if provisioned else "reutilizada"
        print(f"[PROVISION] api_key {action} com sucesso para {server_id}.")
        return True
    except Exception as e:
        print(f"[PROVISION] Falha no provisionamento: {e}")
        return False

# Timeout de autenticação: maior para redes lentas ou backend sob carga.
AUTH_TIMEOUT_SECONDS = 45
# Não usar proxy do ambiente no play.
REQUESTS_NO_PROXY = {"http": None, "https": None}
# Evitar reutilização de conexão: segunda requisição pode travar se o servidor fechar keep-alive.
REQUEST_HEADERS_CONNECTION_CLOSE = {"Connection": "close"}

# Log de falhas de conexão (arquivo): path relativo ao diretório do play.py
_PLAY_CONNECTION_LOG_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'logs')
_PLAY_CONNECTION_LOG_FILE = os.path.join(_PLAY_CONNECTION_LOG_DIR, 'play_connection.log')


def _log_connection_failure(error, stage='auth', extra=None):
    """
    Registra falha de conexão em arquivo e no console.
    stage: 'auth' | 'health' | 'heartbeat' | 'request'
    """
    from datetime import datetime
    exc_type = type(error).__name__
    exc_msg = str(error)
    base_url = globals().get('BASE_URL', '')
    timeout = AUTH_TIMEOUT_SECONDS if stage == 'auth' else extra.get('timeout', '') if extra else ''
    proxy_env = {
        'HTTP_PROXY': os.environ.get('HTTP_PROXY', ''),
        'HTTPS_PROXY': os.environ.get('HTTPS_PROXY', ''),
        'NO_PROXY': os.environ.get('NO_PROXY', ''),
    }
    try:
        machine_id = _get_config_server_id_raw() or os.environ.get('PLAYER_MACHINE_ID', '') or 'LOCAL'
    except Exception:
        machine_id = 'LOCAL'
    ts = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
    line = (
        f"{ts} ERROR [play_connection] machine_id={machine_id} stage={stage} "
        f"base_url={base_url} exc={exc_type} msg={exc_msg!r} timeout={timeout} "
        f"proxy_env={proxy_env}"
    )
    print(f"[LOG] {line}")
    try:
        if not os.path.isdir(_PLAY_CONNECTION_LOG_DIR):
            os.makedirs(_PLAY_CONNECTION_LOG_DIR, exist_ok=True)
        with open(_PLAY_CONNECTION_LOG_FILE, 'a', encoding='utf-8', errors='replace') as f:
            f.write(line + '\n')
    except Exception as e:
        print(f"[LOG] Não foi possível gravar em {_PLAY_CONNECTION_LOG_FILE}: {e}")


def authenticate():
    global auth_token, player_api_key
    max_retries = 5
    retry_delay = 3  # Segundos entre tentativas (rede lenta / backend sob carga)
    player_api_key = player_api_key or get_player_api_key()
    hint_backend_mostrado = False

    for attempt in range(max_retries):
        try:
            if player_api_key:
                print(f"Tentando autenticar com API Key (tentativa {attempt + 1}/{max_retries})")
                try:
                    response = requests.post(
                        f'{BASE_URL}/player/authenticate_api_key',
                        headers={'X-API-Key': player_api_key, **REQUEST_HEADERS_CONNECTION_CLOSE},
                        timeout=AUTH_TIMEOUT_SECONDS,
                        verify=False,
                        proxies=REQUESTS_NO_PROXY,
                    )
                    if response.status_code == 200:
                        try:
                            data = response.json() or {}
                        except ValueError as je:
                            print(f"Resposta não é JSON (URL={BASE_URL}). Verifique se server_url no config.ini aponta para o backend central (ex.: https://app.epr.app.br). Erro: {je}")
                            print(f"Status: {response.status_code}, Corpo (início): {response.text[:200] if response.text else '(vazio)'}")
                            raise
                        # Validar: server_id do config.ini deve corresponder ao cadastro desta API Key
                        api_server_id = (data.get('server_id') or '').strip().upper()
                        config_server_id = _get_config_server_id_raw().strip().upper()
                        if api_server_id and config_server_id and api_server_id != config_server_id:
                            print("")
                            print("*** MAQUINA NAO CORRESPONDE COM O CADASTRO ***")
                            print(f"   O config.ini tem server_id = {_get_config_server_id_raw()!r}")
                            print(f"   Esta API Key esta cadastrada para a maquina: {data.get('server_id')!r}")
                            print("   Altere o config.ini para o server_id da maquina correta ou gere um novo config no painel para esta maquina.")
                            print("")
                            return False
                        # JWT opcional (API key já autentica rotas do player)
                        auth_token = data.get('token') or auth_token
                        print("Autenticação por API Key bem-sucedida!")
                        return True
                    snippet = (response.text or "")[:300] if response.text else "(vazio)"
                    print(f"Erro na autenticação por API Key (status {response.status_code}): {snippet}")
                    if not hint_backend_mostrado and (
                        response.status_code != 401
                        and (
                            response.status_code in (502, 503, 504, 530)
                            or (response.text and ("<!doctype" in response.text.lower() or "<html" in response.text.lower()))
                        )
                    ):
                        _print_backend_unreachable_hint(response.status_code, response.text)
                        hint_backend_mostrado = True
                except ValueError as je:
                    print(f"Resposta do servidor não é JSON (URL={BASE_URL}). Verifique se [Player] server_url no config.ini é o backend central, ex.: https://app.epr.app.br (não https://epr.app.br).")
                    print(f"Erro: {je}")
                    if attempt < max_retries - 1:
                        time.sleep(retry_delay)
                    continue
                except requests.exceptions.RequestException as e:
                    print(f"Erro de conexão: {e}")
                    if not hint_backend_mostrado:
                        _print_backend_unreachable_hint(None, None, e)
                        hint_backend_mostrado = True
                    print(f"Verifique: 1) server_url no config.ini (ex.: https://app.epr.app.br); 2) Backend e túnel no ar.")
                    if "timed out" in str(e).lower() or "timeout" in type(e).__name__.lower():
                        print(f"Dica: Na máquina, teste: curl -I {BASE_URL}/health  ou  ping {BASE_URL.replace('https://','').replace('http://','').split('/')[0]}")
                    _log_connection_failure(e, stage='auth', extra={'timeout': AUTH_TIMEOUT_SECONDS})
                    if attempt < max_retries - 1:
                        print(f"Tentando novamente em {retry_delay} segundos...")
                        time.sleep(retry_delay)
                    continue
                print("Tentando fallback com código do player (config [Player].code ou access_codes.json)...")

            # CRITICAL: player deve autenticar com seu código dedicado para não ocupar vaga de usuário comum.
            access_code = get_player_access_code() or get_valid_access_code()
            if not access_code:
                print("")
                print("Nenhum código de fallback disponível.")
                print("  Configure [Player].code no config.ini (código do player no painel) ou crie access_codes.json,")
                print("  mas se o backend/túnel estiver OFF, o login por código também falhará até o servidor voltar.")
                return False

            # Enviar is_player=True quando usar código do player para reconexão ser aceita (slot já reservado no startup)
            is_player = (access_code == get_player_access_code()) if get_player_access_code() else False
            print(f"Tentando autenticar com código: {access_code} (tentativa {attempt + 1}/{max_retries})")
            response = requests.post(
                f'{BASE_URL}/authenticate',
                json={'access_code': access_code, 'is_permanent': False, 'is_player': is_player},
                headers=REQUEST_HEADERS_CONNECTION_CLOSE,
                timeout=AUTH_TIMEOUT_SECONDS,
                verify=False,
                proxies=REQUESTS_NO_PROXY,
            )

            if response.status_code == 200:
                data = response.json()
                auth_token = data['token']
                print("Autenticação bem-sucedida!")
                return True
            else:
                print(f"Erro na autenticação: {response.text}")
                if attempt < max_retries - 1:
                    print(f"Tentando novamente em {retry_delay} segundos...")
                    time.sleep(retry_delay)
                
        except requests.exceptions.RequestException as e:
            print(f"Erro de conexão: {e}")
            if "timed out" in str(e).lower() or "timeout" in type(e).__name__.lower():
                host = BASE_URL.replace("https://", "").replace("http://", "").split("/")[0]
                print(f"Dica: Na máquina, teste: curl -I {BASE_URL}/health  ou  ping {host}")
            _log_connection_failure(e, stage='auth', extra={'timeout': AUTH_TIMEOUT_SECONDS})
            if attempt < max_retries - 1:
                print(f"Tentando novamente em {retry_delay} segundos...")
                time.sleep(retry_delay)

    print("")
    print("Todas as tentativas de autenticação falharam.")
    print(f"Log detalhado em: {_PLAY_CONNECTION_LOG_FILE}")
    print("Resumo: se viu HTTP 530 ou página HTML em vez de JSON, o problema costuma ser")
    print("  BACKEND DESLIGADO no servidor OU TÚNEL (Cloudflare) DESLIGADO / origem inacessível.")
    print("")
    return False

def get_headers():
    """Headers para requisições ao backend; inclui Connection: close para evitar travar na 2ª requisição."""
    h = dict(REQUEST_HEADERS_CONNECTION_CLOSE)
    if player_api_key:
        h['X-API-Key'] = player_api_key
    elif auth_token:
        h['Authorization'] = f'Bearer {auth_token}'
    return h


def _logout_player_on_exit():
    """Ao encerrar o play.py, notifica o backend para marcar o servidor offline imediatamente."""
    try:
        if (auth_token or player_api_key) and BASE_URL:
            requests.post(
                f'{BASE_URL}/api/logout',
                headers=get_headers(),  # já inclui Connection: close
                timeout=2,
                verify=False,
                proxies=REQUESTS_NO_PROXY,
            )
    except Exception:
        pass


def _signal_handler_sigterm(signum, frame):
    """SIGTERM (Linux): faz logout e encerra."""
    _logout_player_on_exit()
    sys.exit(0)


# SIGTERM: logout ao encerrar por kill/terminate. Ctrl+C não usa handler aqui — tratado no except KeyboardInterrupt.
if getattr(signal, 'SIGTERM', None) is not None:
    signal.signal(signal.SIGTERM, _signal_handler_sigterm)
atexit.register(_logout_player_on_exit)

def check_auth_and_retry(func):
    def wrapper(*args, **kwargs):
        try:
            # Evitar headers duplicado (pode vir do caller e do decorator)
            extra_headers = kwargs.pop('headers', None) if isinstance(kwargs, dict) else None
            headers = get_headers()
            if isinstance(extra_headers, dict):
                if headers and isinstance(headers, dict):
                    headers.update(extra_headers)
                else:
                    headers = extra_headers
            # Desabilitar verificação SSL para todos os requests
            if 'verify' not in kwargs:
                kwargs['verify'] = False
            response = func(*args, headers=headers, **kwargs)
            if response is None:
                return None
            if response.status_code == 401:
                print("Token expirado ou inválido. Tentando autenticar novamente...")
                if authenticate():
                    headers = get_headers()
                    response = func(*args, headers=headers, **kwargs)
                else:
                    print("Falha na autenticação")
                    return None
            return response
        except requests.exceptions.RequestException as e:
            print(f"Erro na requisição: {e}")
            if getattr(e.response, 'status_code', None) == 401:
                if authenticate():
                    headers = get_headers()
                    return func(*args, headers=headers, **kwargs)
            return None
    return wrapper

@check_auth_and_retry
def make_request(url, method="GET", headers=None, timeout=2, **kwargs):
    # Garantir que headers seja passado apenas uma vez (pode vir via kwargs e argumento explícito)
    if isinstance(kwargs, dict) and "headers" in kwargs:
        extra_headers = kwargs.pop("headers")
        if isinstance(extra_headers, dict):
            if headers and isinstance(headers, dict):
                headers.update(extra_headers)
            else:
                headers = extra_headers

    # Permite que chamadas específicas (ex.: update_video_progress) usem timeout maior.
    disable_timeout_clamp = False
    if isinstance(kwargs, dict) and "disable_timeout_clamp" in kwargs:
        try:
            disable_timeout_clamp = bool(kwargs.pop("disable_timeout_clamp"))
        except Exception:
            disable_timeout_clamp = False
    # Normalizar timeout (pode vir em kwargs também)
    if isinstance(kwargs, dict) and "timeout" in kwargs:
        try:
            timeout = kwargs.pop("timeout") or timeout
        except Exception:
            kwargs.pop("timeout", None)
    # Circuit breaker: evitar tempestade de requests quando backend está instável/offline.
    # Telemetria de progresso (`/update_video_progress`) é menos crítica para "não travar fila",
    # então permitimos tentar mesmo quando o CB estiver aberto (evita app "sumir" o timer).
    try:
        if isinstance(url, str) and url.startswith(BASE_URL) and not _backend_can_try():
            allow_progress_http = ('/update_video_progress' in url)
            if not allow_progress_http:
                # Não remover logs existentes; apenas complementar.
                _cb_log_skip()
                return None
    except Exception:
        pass
    # Desabilitar verificação SSL e não usar proxy.
    kwargs['verify'] = False
    kwargs.setdefault('proxies', REQUESTS_NO_PROXY)

    # Respeitar o timeout informado pelo chamador para URLs do backend.
    # O clamp fixo de 2s derrubava /get_videos em ambiente remoto, abria o circuit breaker
    # e fazia o player esconder a janela como se a playlist estivesse vazia.
    try:
        if isinstance(url, str) and url.startswith(BASE_URL):
            try:
                current_timeout = timeout
                # Aceita float/int; se vier string, ignora.
                if isinstance(current_timeout, (int, float)):
                    if disable_timeout_clamp:
                        # Para progresso: permitir timeout um pouco maior, mas ainda limitado.
                        timeout = max(1, min(6, float(current_timeout)))
                    else:
                        timeout = max(1, min(30, float(current_timeout)))
                else:
                    timeout = REQUEST_TIMEOUT_GET_VIDEOS_SEC if method.lower() == 'get' else REQUEST_TIMEOUT_ADD_SEC
            except Exception:
                timeout = REQUEST_TIMEOUT_GET_VIDEOS_SEC if method.lower() == 'get' else REQUEST_TIMEOUT_ADD_SEC
    except Exception:
        pass

    # Se não for URL do backend (ex.: Redis/externo), respeitar timeout default
    if not (isinstance(url, str) and url.startswith(BASE_URL)):
        if method.lower() == 'get':
            timeout = timeout or REQUEST_TIMEOUT_GET_VIDEOS_SEC
        elif method.lower() == 'post':
            timeout = timeout or REQUEST_TIMEOUT_ADD_SEC

    # Garantir Connection: close em toda requisição ao backend (evita travar na 2ª).
    h = dict(headers) if headers else {}
    h.setdefault('Connection', 'close')
    if method.lower() == 'get':
        return requests.get(url, headers=h, timeout=timeout, **kwargs)
    elif method.lower() == 'post':
        return requests.post(url, headers=h, timeout=timeout, **kwargs)
    return None

def get_video_list():
    global queue_fetch_last_ok, queue_first_fetch_pending
    # Circuit breaker: enquanto desabilitado, não tenta backend nem "primeiro load" longo.
    base_timeout = REQUEST_TIMEOUT_GET_VIDEOS_FIRST_LOAD_SEC if queue_first_fetch_pending else REQUEST_TIMEOUT_GET_VIDEOS_SEC
    backoff_sec = 0.5
    last_error = None

    # Se backend está em circuit breaker, não insistir; ir direto ao cache local.
    if not _backend_can_try():
        _cb_log_skip()
        cached_videos = _load_queue_cache()
        if cached_videos is not None:
            queue_fetch_last_ok = True
            if cached_videos:
                queue_first_fetch_pending = False
                print(f"[TIMING] QUEUE_ITEMS_RECEIVED items={len(cached_videos)} source=local_cache circuit_breaker=1")
            return cached_videos
        queue_fetch_last_ok = False
        return None

    for attempt in range(1, QUEUE_FETCH_MAX_RETRIES + 1):
        if queue_first_fetch_pending:
            timeout_now = base_timeout if attempt == 1 else REQUEST_TIMEOUT_GET_VIDEOS_SEC
        else:
            timeout_now = REQUEST_TIMEOUT_GET_VIDEOS_SEC
        t0 = time.perf_counter()
        response = None
        try:
            response = make_request(SERVER_URL, timeout=timeout_now)
            if response and response.status_code == 200:
                data = response.json()
                items = data if isinstance(data, list) else []
                _save_queue_cache(items)
                queue_fetch_last_ok = True
                queue_first_fetch_pending = False
                _backend_record_success()
                elapsed_ms = (time.perf_counter() - t0) * 1000
                count = len(items)
                print(f"[TIMING] QUEUE_ITEMS_RECEIVED items={count} elapsed_ms={elapsed_ms:.1f} timeout_s={timeout_now} attempt={attempt}")
                return items

            elapsed_ms = (time.perf_counter() - t0) * 1000
            status = getattr(response, 'status_code', 'N/A')
            last_error = f"HTTP {status}"
            print(f"[TIMING] QUEUE_FETCH_TIMEOUT status={status} elapsed_ms={elapsed_ms:.1f} timeout_s={timeout_now} attempt={attempt}")
            _backend_record_failure()
        except Exception as e:
            elapsed_ms = (time.perf_counter() - t0) * 1000
            last_error = str(e)
            print(f"[TIMING] QUEUE_FETCH_TIMEOUT error={type(e).__name__} elapsed_ms={elapsed_ms:.1f} timeout_s={timeout_now} attempt={attempt}")
            _backend_record_failure()

        if attempt < QUEUE_FETCH_MAX_RETRIES:
            print(f"[TIMING] QUEUE_FETCH_RETRY attempt={attempt + 1} backoff_s={backoff_sec:.1f}")
            time.sleep(backoff_sec)
            backoff_sec = min(4.0, backoff_sec * 2)

    cached_videos = _load_queue_cache()
    if cached_videos is not None:
        queue_fetch_last_ok = True
        if cached_videos:
            queue_first_fetch_pending = False
            print(f"[TIMING] QUEUE_ITEMS_RECEIVED items={len(cached_videos)} source=local_cache")
        return cached_videos

    queue_fetch_last_ok = False
    print(f"[TIMING] QUEUE_FETCH_HTTP_FAIL reason={last_error or 'unknown'}")
    # Falha de rede/timeout: não retornar [] para não apagar playlist local no update_playlist()
    return None


def get_skip_requested():
    """Consulta o servidor se um admin solicitou pular o vídeo (app Configurações)."""
    try:
        # Se o circuit breaker está aberto, o backend provavelmente está indisponível.
        # Evita ficar bloqueado em timeouts e atrasar a reação do player.
        if not _backend_can_try():
            _cb_log_skip()
            return False
        response = make_request(SKIP_REQUESTED_URL, timeout=REQUEST_TIMEOUT_SKIP_SEC)
        if response and response.status_code == 200:
            data = response.json()
            return data.get('skip') is True
        return False
    except Exception:
        return False


def restart_edge_progress_socketio():
    """
    Desconecta o cliente Socket.IO e sobe de novo (ex.: após renovar JWT no heartbeat).
    Evita ficar com sessão morta após falhas de rede ou token expirado.
    """
    global edge_progress_socketio_client, edge_progress_socketio_thread
    with edge_progress_socketio_lock:
        old_t = edge_progress_socketio_thread
        try:
            if edge_progress_socketio_client is not None:
                edge_progress_socketio_client.disconnect()
        except Exception:
            pass
        edge_progress_socketio_client = None
        edge_progress_socketio_connected_event.clear()
        if old_t is not None and old_t.is_alive():
            try:
                old_t.join(timeout=8)
            except Exception:
                pass
        edge_progress_socketio_thread = None
    init_edge_progress_socketio()


def init_edge_progress_socketio():
    """
    Inicializa um cliente Socket.IO para enviar progresso do edge (play.py) ao backend.
    Se falhar (lib ausente/rede), mantém fallback HTTP em update_video_progress().
    """
    global edge_progress_socketio_client, edge_progress_socketio_thread
    try:
        import socketio  # python-socketio
    except Exception:
        return

    if edge_progress_socketio_thread and edge_progress_socketio_thread.is_alive():
        return

    def _runner():
        global edge_progress_socketio_client
        try:
            if not auth_token:
                return
            # Se websocket-client não estiver instalado, forçamos polling.
            transports = ["websocket", "polling"]
            try:
                import websocket  # websocket-client  # type: ignore
            except Exception:
                transports = ["polling"]

            # reconnection_attempts=0 (padrão da lib) = reconexões ilimitadas.
            # Valores >0 fazem o cliente desistir após N falhas ("giving up") e parar de tentar para sempre.
            client = socketio.Client(
                reconnection=True,
                reconnection_attempts=0,
                reconnection_delay=1.0,
                reconnection_delay_max=30.0,
                logger=False,
                engineio_logger=False,
                request_timeout=3,
            )

            @client.event
            def connect():
                edge_progress_socketio_connected_event.set()
                print("[PROGRESS_SOCKETIO] conectado ao backend")

            @client.event
            def disconnect():
                edge_progress_socketio_connected_event.clear()
                print("[PROGRESS_SOCKETIO] desconectado do backend")

            @client.on('videoListUpdated')
            def _on_video_list_updated(data):
                """
                Backend notifica que a fila mudou.
                Dispara refresh via fluxo existente (update_playlist/get_video_list HTTP).
                """
                payload = data if isinstance(data, dict) else {}
                event_server_id = (payload.get('server_id') or '').strip()
                current_server_id = (_get_current_server_id() or '').strip()
                if event_server_id and current_server_id and event_server_id.upper() != current_server_id.upper():
                    return
                print("[QUEUE] videoListUpdated recebido via Socket.IO - recarregando fila")
                playlist_refresh_event.set()

            @client.on('skipRequested')
            def _on_skip_requested(data):
                """
                Backend notifica skip em tempo real via Socket.IO.
                Mantemos GET /api/skip_requested para consumir flag com idempotência.
                """
                payload = data if isinstance(data, dict) else {}
                event_server_id = (payload.get('server_id') or '').strip()
                current_server_id = (_get_current_server_id() or '').strip()
                if event_server_id and current_server_id and event_server_id.upper() != current_server_id.upper():
                    return
                print("[SKIP] skipRequested recebido via Socket.IO")
                skip_requested_socket_event.set()

            edge_progress_socketio_client = client
            # token via auth é suportado pelo client (assim como no frontend)
            client.connect(
                BASE_URL,
                auth={"token": auth_token},
                transports=transports,
                wait_timeout=10,
            )
            # Mantém thread viva
            client.wait()
        except Exception as e:
            edge_progress_socketio_connected_event.clear()
            try:
                if edge_progress_socketio_client is not None:
                    edge_progress_socketio_client.disconnect()
            except Exception:
                pass
            # Silencioso: fallback HTTP cobre
            print(f"[PROGRESS_SOCKETIO] init falhou: {type(e).__name__}: {e}")

    edge_progress_socketio_thread = threading.Thread(target=_runner, daemon=True)
    edge_progress_socketio_thread.start()


def _parse_video_info(info, max_height):
    """Extrai (video_url, audio_url) do info retornado pelo yt-dlp. audio_url pode ser None."""
    if not info:
        return None
    if 'requested_formats' in info and len(info['requested_formats']) > 1:
        video_url = info['requested_formats'][0]['url']
        audio_url = info['requested_formats'][1]['url']
        return (video_url, audio_url)
    video_url = info.get('url')
    if not video_url:
        return None
    return (video_url, None)


def _get_stream_height(info):
    """Retorna a altura em pixels do stream selecionado (para log). None se não disponível."""
    if not info:
        return None
    if 'requested_formats' in info and len(info['requested_formats']) > 0:
        return info['requested_formats'][0].get('height') or info['requested_formats'][0].get('resolution')
    return info.get('height')

def _is_backend_local():
    """
    True se BASE_URL aponta para a mesma máquina ou rede local (localhost, 127.0.0.1, IP privado).
    Usado para decidir automaticamente se vale tentar GET /get_video_url no backend.
    """
    try:
        from urllib.parse import urlparse
        p = urlparse(BASE_URL)
        host = (p.hostname or '').strip().lower()
        if not host or host in ('localhost', '127.0.0.1', '::1'):
            return True
        # IP privados: 10.x, 172.16–31.x, 192.168.x
        if host.startswith('10.'):
            return True
        if host.startswith('192.168.'):
            return True
        if host.startswith('172.'):
            parts = host.split('.')
            if len(parts) >= 2:
                try:
                    second = int(parts[1])
                    if 16 <= second <= 31:
                        return True
                except ValueError:
                    pass
        return False
    except Exception:
        return False


def get_video_url_from_backend(video_id, timeout_seconds=2):
    """
    Obtém URL do vídeo via backend (GET /get_video_url/<id>).
    Usa ?hd=1 para o backend ignorar cache e buscar sempre em HD (prioridade 1080p→720p→…).
    Retorna (url, None) pois o backend devolve stream único (merged); ou None em falha/timeout.
    """
    headers = get_headers()
    if not headers:
        return None
    try:
        if not _backend_can_try():
            _cb_log_skip()
            return None
        resp = requests.get(
            f'{BASE_URL}/get_video_url/{video_id}?hd=1',
            headers=headers,
            timeout=timeout_seconds,
            verify=False,
            proxies=REQUESTS_NO_PROXY,
        )
        if resp.status_code == 200:
            data = resp.json()
            url = data.get('url') if isinstance(data, dict) else None
            if url and isinstance(url, str) and url.startswith(('http://', 'https://')):
                _backend_record_success()
                return (url, None)
        _backend_record_failure()
        return None
    except requests.exceptions.Timeout:
        _backend_record_failure()
        return None
    except Exception as e:
        _backend_record_failure()
        print(f"Backend get_video_url: {type(e).__name__}: {e}")
        return None


def _extract_stream_expire_epoch(url: str):
    """Extrai o `expire` da URL assinada e retorna epoch em segundos."""
    if not url or not isinstance(url, str):
        return None
    try:
        from urllib.parse import urlparse, parse_qs
        parsed = urlparse(url)
        params = parse_qs(parsed.query or "")
        values = params.get("expire")
        if not values:
            return None
        return int(values[0])
    except Exception:
        return None


def _compute_stream_expires_at(video_url: str, audio_url: str | None = None):
    """
    Define expiração do stream local.
    - Se URL assinada trouxer `expire`, usa o menor expire com margem de segurança.
    - Caso contrário, TTL curto default.
    """
    now = int(time.time())
    candidates = []
    for u in (video_url, audio_url):
        exp = _extract_stream_expire_epoch(u) if u else None
        if exp:
            candidates.append(exp)
    if candidates:
        # margem para não tentar usar stream no limite
        return max(now + 20, min(candidates) - 90)
    return now + STREAM_CACHE_DEFAULT_TTL_SEC


def _normalize_cached_stream_entry(entry):
    """
    Compatibilidade:
    - novo formato: {'stream': (video_url, audio_url), 'expires_at': int}
    - formato legado: (video_url, audio_url)
    """
    if isinstance(entry, dict):
        stream = entry.get('stream')
        if isinstance(stream, tuple) and len(stream) == 2:
            return entry
        return None
    if isinstance(entry, tuple) and len(entry) == 2:
        # legado: converter para novo formato com TTL curto
        return {
            'stream': entry,
            'expires_at': int(time.time()) + STREAM_CACHE_DEFAULT_TTL_SEC,
            'source': 'legacy',
            'cached_at': int(time.time()),
        }
    return None


def _is_stream_entry_valid(entry, min_validity_sec=STREAM_MIN_VALIDITY_SEC):
    if not entry:
        return False
    exp = entry.get('expires_at')
    if exp is None:
        return False
    return int(time.time()) + int(min_validity_sec) < int(exp)


def _cache_stream(video_id, stream, source='unknown', ttl_seconds=None):
    if not video_id or not stream:
        return
    video_url, audio_url = stream
    if ttl_seconds is not None:
        expires_at = int(time.time()) + int(max(20, ttl_seconds))
    else:
        expires_at = _compute_stream_expires_at(video_url, audio_url)
    media_dict[video_id] = {
        'stream': (video_url, audio_url),
        'expires_at': int(expires_at),
        'source': source,
        'cached_at': int(time.time()),
    }


def _get_cached_stream(video_id, min_validity_sec=STREAM_MIN_VALIDITY_SEC):
    entry = _normalize_cached_stream_entry(media_dict.get(video_id))
    if not entry:
        return None
    if not _is_stream_entry_valid(entry, min_validity_sec=min_validity_sec):
        media_dict.pop(video_id, None)
        return None
    # persistir conversão de legado para o formato novo
    media_dict[video_id] = entry
    return entry.get('stream')


def clear_stream_cache(video_id):
    media_dict.pop(video_id, None)


def _resolve_stream_local_ytdlp(video_id):
    """Resolve stream localmente com yt-dlp (fallback resiliente quando backend/redis estão indisponíveis)."""
    url = f'https://www.youtube.com/watch?v={video_id}'
    max_height = VIDEO_QUALITY_CONFIG.get('max_height', 0)
    cap = max_height if (max_height and max_height > 0) else 1080
    # Ordem de formatos: tenta mais qualidade e degrada para aumentar chance de sucesso em vídeos restritos.
    format_candidates = [
        f'bestvideo[height<={cap}]+bestaudio/best',
        f'best[height<={cap}]/best',
        'best',
    ]
    print(f"Obtendo stream local do vídeo {video_id} (yt-dlp, até {cap}p)...")

    base_opts = {
        'quiet': True,
        'no_warnings': True,
        'nocheckcertificate': True,
        'force_ipv4': True,
        'socket_timeout': 30,
        'retries': 5,
        'no_playlist': True,
    }

    try:
        ssl._create_default_https_context = ssl._create_unverified_context
    except Exception:
        pass

    last_error = None
    needs_cookies = bool(YTDLP_CONFIG.get('try_cookies_first', False))

    # 1) Tentativas sem forçar cookies (ainda respeita cookies_file/cookies_from_browser do config)
    if not needs_cookies:
        for fmt in format_candidates:
            ydl_opts = dict(base_opts)
            ydl_opts['format'] = fmt
            # "Sem cookies" de verdade (não herdar cookies_from_browser do config)
            _apply_ydl_auth_options(
                ydl_opts,
                include_cookie_file=False,
                include_browser_cookies=False
            )
            try:
                with yt_dlp.YoutubeDL(ydl_opts) as ydl:
                    info = ydl.extract_info(url, download=False)
                result = _parse_video_info(info, max_height or 9999)
                if result:
                    res = _get_stream_height(info)
                    res_msg = f" {res}p" if res else ""
                    print(f"Stream local obtido ({fmt}){res_msg}")
                    return result
            except Exception as e:
                last_error = e
                err_lower = str(e).lower()
                if (
                    'not a bot' in err_lower
                    or 'sign in to confirm' in err_lower
                    or 'cookies-from-browser' in err_lower
                    or 'video is not available' in err_lower
                ):
                    needs_cookies = True
                    break
                # formato indisponível: tenta próximo formato
                if 'requested format is not available' in err_lower:
                    continue
                # outros erros também tentam próximo formato (rede intermitente)
                continue

    # 2) Tentativas com cookies (configurável, sem fixar firefox)
    if needs_cookies:
        print("Tentando com cookies/navegador (vídeo restrito)...")
        browser_from_cfg = (YTDLP_CONFIG.get('cookies_from_browser') or '').strip()
        profile = (YTDLP_CONFIG.get('cookies_profile') or '').strip()
        browser_candidates = _parse_csv(browser_from_cfg) if browser_from_cfg else []
        # fallback útil no Windows: tenta navegadores comuns além do configurado
        for fallback_browser in ('firefox', 'chrome', 'edge'):
            if fallback_browser not in browser_candidates:
                browser_candidates.append(fallback_browser)
        # tenta também sem browser fixo para usar apenas cookies_file (se existir)
        browser_candidates.append('')

        # Monta tentativas com e sem profile fixo para evitar travar em profile inexistente ("Default")
        attempt_variants = []
        for browser in browser_candidates:
            if browser:
                if profile:
                    attempt_variants.append((browser, profile))
                attempt_variants.append((browser, None))
            else:
                attempt_variants.append(('', None))

        for browser, profile_override in attempt_variants:
            for fmt in format_candidates:
                ydl_opts_cookies = dict(base_opts)
                ydl_opts_cookies['format'] = fmt
                ydl_opts_cookies['extractor_args'] = {'youtube': {'player_client': ['web']}}

                # aplicar config global sem herdar cookiesfrombrowser do config (definimos por tentativa)
                _apply_ydl_auth_options(
                    ydl_opts_cookies,
                    include_cookie_file=True,
                    include_browser_cookies=False
                )

                # se browser explícito nesta tentativa, sobrescreve cookiesfrombrowser
                if browser:
                    if profile_override:
                        ydl_opts_cookies['cookiesfrombrowser'] = (browser, profile_override)
                    else:
                        ydl_opts_cookies['cookiesfrombrowser'] = (browser,)
                    if profile_override:
                        print(f"[yt] Tentando cookies do navegador: {browser} profile={profile_override} (format={fmt})")
                    else:
                        print(f"[yt] Tentando cookies do navegador: {browser} (sem profile fixo) (format={fmt})")
                else:
                    print(f"[yt] Tentando apenas cookies_file (sem browser fixo) (format={fmt})")

                try:
                    with yt_dlp.YoutubeDL(ydl_opts_cookies) as ydl:
                        info = ydl.extract_info(url, download=False)
                    result = _parse_video_info(info, max_height or 9999)
                    if result:
                        res = _get_stream_height(info)
                        res_msg = f" {res}p" if res else ""
                        print(f"Stream local com cookies obtido ({fmt}){res_msg}")
                        return result
                except Exception as e:
                    last_error = e
                    if _is_cookie_db_missing_error(str(e)):
                        if browser:
                            print(f"[yt] Banco de cookies não encontrado para browser={browser}.")
                        else:
                            print("[yt] Sem cookies_file válido para esta tentativa.")
                        print("[yt] Ajuste [YtDlp] cookies_profile para o profile real do navegador, ou use cookies_file.")
                        # Não insistir com os mesmos formatos quando não há DB de cookies.
                        break
                    # segue tentativas
                    continue

    if last_error:
        err_text = str(last_error)
        print(f"Erro local yt-dlp (cookies) {video_id}: {type(last_error).__name__}: {err_text}")
        err_lower = err_text.lower()
        if 'sign in to confirm your age' in err_lower:
            print("[yt] Bloqueio de idade detectado: cookies do navegador não estão válidos para este vídeo/conta.")
            print("[yt] Verifique no config.ini [YtDlp]: cookies_from_browser (ex.: firefox), cookies_profile, ou use cookies_file exportado.")
        elif 'requested format is not available' in err_lower:
            print("[yt] Formato indisponível mesmo com fallback. Pode ser vídeo restrito/região/idade ou alteração de formatos no YouTube.")
    return None


def resolve_stream(video_id, force_refresh=False, min_validity_sec=STREAM_MIN_VALIDITY_SEC):
    """
    Resolve stream JIT com prioridade:
      1) cache local válido (não expirado)
      2) backend /get_video_url
      3) yt-dlp local (fallback resiliente)
    """
    if not video_id:
        return None

    with video_locks[video_id]:
        if not force_refresh:
            cached = _get_cached_stream(video_id, min_validity_sec=min_validity_sec)
            if cached:
                print(f"Stream em cache válido para {video_id}")
                log_event("CACHE_HIT", {"video_id": video_id, "layer": "local_stream"})
                return cached

    inflight_entry = None
    is_owner = False
    with _stream_resolve_inflight_lock:
        inflight_entry = _stream_resolve_inflight.get(video_id)
        if inflight_entry is None:
            inflight_entry = {"event": threading.Event(), "result": None}
            _stream_resolve_inflight[video_id] = inflight_entry
            is_owner = True
    if not is_owner:
        inflight_entry["event"].wait(timeout=35)
        with video_locks[video_id]:
            if not force_refresh:
                cached = _get_cached_stream(video_id, min_validity_sec=min_validity_sec)
                if cached:
                    print(f"Stream em cache válido para {video_id}")
                    log_event("CACHE_HIT", {"video_id": video_id, "layer": "local_stream"})
                    return cached
        waiter_result = inflight_entry.get("result")
        if waiter_result:
            return waiter_result
        return None

    try:
        log_event("CACHE_MISS", {"video_id": video_id, "layer": "local_stream"})

        # Em arquitetura distribuída, priorizar backend central para manter consistência global.
        stream = get_video_url_from_backend(video_id)
        source = 'backend'
        if not stream:
            # Fallback resiliente no edge: player resolve sozinho via yt-dlp local
            stream = _resolve_stream_local_ytdlp(video_id)
            source = 'local_ytdlp'
        if stream:
            with video_locks[video_id]:
                if not force_refresh:
                    cached = _get_cached_stream(video_id, min_validity_sec=min_validity_sec)
                    if cached:
                        print(f"Stream em cache válido para {video_id}")
                        log_event("CACHE_HIT", {"video_id": video_id, "layer": "local_stream"})
                        with _stream_resolve_inflight_lock:
                            current = _stream_resolve_inflight.get(video_id)
                            if current is inflight_entry:
                                current["result"] = cached
                                current["event"].set()
                                _stream_resolve_inflight.pop(video_id, None)
                        return cached
                _cache_stream(video_id, stream, source=source)
                with _stream_resolve_inflight_lock:
                    current = _stream_resolve_inflight.get(video_id)
                    if current is inflight_entry:
                        current["result"] = stream
                        current["event"].set()
                        _stream_resolve_inflight.pop(video_id, None)
                return stream

        log_event("STREAM_RESOLVE_FAIL", {"video_id": video_id, "force_refresh": bool(force_refresh)})
        with _stream_resolve_inflight_lock:
            current = _stream_resolve_inflight.get(video_id)
            if current is inflight_entry:
                current["result"] = None
                current["event"].set()
                _stream_resolve_inflight.pop(video_id, None)
        return None
    except Exception:
        with _stream_resolve_inflight_lock:
            current = _stream_resolve_inflight.get(video_id)
            if current is inflight_entry:
                current["result"] = None
                current["event"].set()
                _stream_resolve_inflight.pop(video_id, None)
        raise


def get_video_url(video_id, force_refresh=False, min_validity_sec=STREAM_MIN_VALIDITY_SEC):
    """Compatibilidade: alias para resolve_stream()."""
    return resolve_stream(video_id, force_refresh=force_refresh, min_validity_sec=min_validity_sec)


def _extract_video_id_from_item(video):
    """Compatibilidade: aceita payload legado (`id`) e novo (`video_id`)."""
    if not isinstance(video, dict):
        return None
    if video.get('video_id'):
        return video.get('video_id')
    if video.get('videoId'):
        return video.get('videoId')
    raw_id = video.get('id')
    if isinstance(raw_id, dict):
        return raw_id.get('videoId') or raw_id.get('video_id')
    if isinstance(raw_id, str):
        return raw_id
    return None


def _remove_from_playlist_by_unique_id(unique_id):
    """
    Remove da playlist o item com o uniqueId dado (não por índice).
    Assim a ordem é respeitada mesmo se update_playlist() tiver reordenado durante a reprodução.
    Retorna o item removido ou None.
    """
    global playlist
    with _playlist_lock:
        if not unique_id or not playlist:
            return None
        for i, p in enumerate(playlist):
            if p.get('uniqueId') == unique_id:
                removed = playlist.pop(i)
                return removed
    return None


def update_playlist():
    global playlist
    try:
        videos = get_video_list()
        if videos is None:
            print("[QUEUE] update_playlist: mantendo playlist local (fetch falhou)")
            return
        if not isinstance(videos, list):
            print("Erro: Lista de vídeos inválida recebida do servidor")
            return

        # Evitar atualização desnecessária quando a fila não mudou (ordem + uniqueIds).
        # Importante: não remove logs existentes; apenas complementa.
        try:
            uids = []
            for v in videos:
                uid = (v.get('uniqueId') if isinstance(v, dict) else None) or (v.get('unique_id') if isinstance(v, dict) else None)
                if uid:
                    uids.append(str(uid))
            h = hashlib.sha1(('|'.join(uids)).encode('utf-8', errors='ignore')).hexdigest()
            global last_server_queue_hash, last_server_queue_len
            changed = (h != last_server_queue_hash) or (last_server_queue_len != len(videos))
            print(f"[QUEUE_HASH] changed={str(changed).lower()} items={len(videos)}")
            if not changed:
                print("[QUEUE] No changes detected, skipping update")
                return
            last_server_queue_hash = h
            last_server_queue_len = len(videos)
        except Exception:
            pass

        # CRITICAL: Criar mapa de uniqueIds dos vídeos do servidor
        server_unique_ids = {}
        for video in videos:
            try:
                unique_id = video.get('uniqueId') or video.get('unique_id')
                if unique_id:
                    server_unique_ids[unique_id] = video
            except Exception:
                continue
        
        with _playlist_lock:
            # CRITICAL: Remover vídeos da playlist local que não estão mais no servidor
            # Isso evita que vídeos já removidos sejam mantidos na playlist
            original_playlist_size = len(playlist)
            current_uid = (current_playing_video or {}).get('uniqueId')
            playlist = [
                p for p in playlist
                if p.get('uniqueId') in server_unique_ids
                or (current_uid and p.get('uniqueId') == current_uid)
            ]
            removed_count = original_playlist_size - len(playlist)
            if removed_count > 0:
                print(f"🗑️ {removed_count} vídeo(s) removido(s) da playlist local (não estão mais no servidor)")

            # Criar uma lista de uniqueIds dos vídeos já na playlist local
            playlist_unique_ids = set(
                p.get('uniqueId') for p in playlist if p.get('uniqueId')
            )
            
            # CRITICAL: Adicionar apenas vídeos novos pelo uniqueId, na ordem do servidor
            # Manter a ordem original do banco (ORDER BY position ASC)
            new_videos_added = 0
            for video in videos:
                try:
                    video_id = _extract_video_id_from_item(video)
                    unique_id = video.get('uniqueId') or video.get('unique_id')
                    
                    # Verificar pelo uniqueId para permitir vídeos repetidos na fila
                    if video_id and unique_id and unique_id not in playlist_unique_ids:
                        playlist.append({
                            # fila local mantém id normalizado para compatibilidade com o restante do player
                            'id': {'videoId': video_id},
                            'video_id': video_id,
                            'uniqueId': unique_id,
                            'title': video.get('title', 'Sem título'),
                            'addedAt': video.get('addedAt', time.time() * 1000)
                        })
                        playlist_unique_ids.add(unique_id)
                        new_videos_added += 1
                        print(f"✅ Novo vídeo adicionado à playlist (uniqueId: {unique_id}): {video.get('title', video_id)}")
                    elif not unique_id:
                        print(f"⚠️ Vídeo sem uniqueId ignorado: {video.get('title', video_id)}")
                except Exception as e:
                    print(f"Erro ao processar vídeo: {e}")
                    continue

            # CRITICAL: Reordenar playlist para corresponder à ordem do servidor
            # Criar mapa de posição baseado na ordem do servidor
            server_order = {}
            for idx, video in enumerate(videos):
                uid = video.get('uniqueId') or video.get('unique_id')
                if uid:
                    server_order[uid] = idx
            
            # Ordenar playlist local pela ordem do servidor (mantém ordem original do banco)
            # Vídeos que não estão no servidor vão para o final (999999)
            playlist.sort(key=lambda x: server_order.get(x.get('uniqueId'), 999999))
            
            # CRITICAL: Remover vídeos que não estão mais no servidor (posição 999999)
            playlist = [
                p for p in playlist
                if p.get('uniqueId') in server_order
                or (current_uid and p.get('uniqueId') == current_uid)
            ]
            
            # Durante reprodução: manter o vídeo que está tocando no índice 0 (evita confusão com pré-carregamento e remoção)
            try:
                curr = current_playing_video
                if curr and playlist and curr.get('uniqueId'):
                    uid = curr.get('uniqueId')
                    idx = next((i for i, p in enumerate(playlist) if p.get('uniqueId') == uid), None)
                    if idx is not None and idx != 0:
                        item = playlist.pop(idx)
                        playlist.insert(0, item)
            except Exception:
                pass
            
            if playlist:
                print(f"📋 Playlist atualizada. Total: {len(playlist)} vídeos ({new_videos_added} novos). Próximo: {playlist[0].get('title', 'N/A')}")

    except Exception as e:
        print(f"Erro ao atualizar playlist: {e}")

def update_video_progress(video_id: str, current_time: int, duration: int, unique_id: str = None):
    try:
        server_id = _get_current_server_id()
        progress_data = {
            'videoId': video_id,
            'currentTime': current_time,
            'duration': duration,
            'uniqueId': unique_id,
            'server_id': server_id,
        }

        # Caminho event-driven (Socket.IO): menor latência e menor overhead.
        try:
            if (
                edge_progress_socketio_client is not None
                and edge_progress_socketio_connected_event.is_set()
            ):
                # Emite para backend, que reemite ao app (com throttle/dedupe).
                edge_progress_socketio_client.emit('edge_video_progress', progress_data)
                return
        except Exception:
            # Se socket falhar, cai para HTTP como fallback.
            pass

        # Fallback HTTP (compatibilidade / offline)
        headers = get_headers()
        response = make_request(
            f'{BASE_URL}/update_video_progress',
            method='post',
            json={
                'videoId': video_id,
                'currentTime': current_time,
                'duration': duration,
                'uniqueId': unique_id,
            },
            headers=headers,
            timeout=6,
            disable_timeout_clamp=True,
        )
        if response is None:
            return
        if response.status_code == 200:
            # silencioso: evita spam no console com telemetria alta
            return
        if response.status_code == 429:
            return
    except Exception as e:
        print(f"Erro ao enviar progresso: {e}")

def remove_media_from_list(video_id):
    clear_stream_cache(video_id)

# ==== Remoções pendentes persistidas (evita reintrodução após restart) ====
_pending_removals_lock = threading.RLock()
_PENDING_REMOVALS_FILENAME = 'pending_removals.json'
_PENDING_REMOVALS_WORKER_INTERVAL_SEC = 15

def _get_pending_removals_path() -> str:
    """Caminho local para persistir remoções pendentes entre reinícios."""
    try:
        base = os.path.dirname(os.path.abspath(__file__))
    except NameError:
        # Alguns ambientes executam código sem __file__ (evitar crash).
        base = os.getcwd()
    return os.path.join(base, _PENDING_REMOVALS_FILENAME)

def _load_pending_removals() -> dict:
    path = _get_pending_removals_path()
    try:
        with _pending_removals_lock:
            if not os.path.exists(path):
                return {}
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f) or {}
            return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _save_pending_removals(removals: dict) -> None:
    path = _get_pending_removals_path()
    try:
        with _pending_removals_lock:
            tmp_path = f"{path}.tmp"
            with open(tmp_path, 'w', encoding='utf-8') as f:
                json.dump(removals, f, ensure_ascii=False, indent=2)
            os.replace(tmp_path, path)
    except Exception:
        pass

def _enqueue_pending_removal(video_id: str, unique_id: str, reason: str) -> None:
    if not unique_id:
        return
    unique_id = str(unique_id)
    video_id = str(video_id) if video_id is not None else ''
    now = time.time()

    with _pending_removals_lock:
        removals = _load_pending_removals()
        entry = removals.get(unique_id) or {}
        attempts = int(entry.get('attempts') or 0)
        attempts = attempts + 1
        # Backoff simples para reduzir tentativas agressivas.
        backoff_sec = min(300, max(5, attempts * 5))
        removals[unique_id] = {
            'video_id': video_id,
            'reason': reason,
            'attempts': attempts,
            'last_error_at': now,
            'next_retry_at': now + backoff_sec,
        }
        _save_pending_removals(removals)
    print(f"[PENDING_REMOVE] queued uniqueId={unique_id} attempts={attempts} reason={reason}")

def _mark_pending_removal_success(unique_id: str) -> None:
    if not unique_id:
        return
    unique_id = str(unique_id)
    with _pending_removals_lock:
        removals = _load_pending_removals()
        if unique_id in removals:
            removals.pop(unique_id, None)
            _save_pending_removals(removals)

def pending_removals_worker():
    """Worker que retenta remoções falhas (ex.: circuit breaker / timeouts)."""
    while True:
        try:
            removals = _load_pending_removals()
            if not removals:
                time.sleep(_PENDING_REMOVALS_WORKER_INTERVAL_SEC)
                continue

            now = time.time()
            # Evitar muita carga: tentar só o que já passou do retry_at.
            due_items = []
            for uid, entry in removals.items():
                try:
                    next_retry_at = float(entry.get('next_retry_at') or 0)
                except Exception:
                    next_retry_at = 0
                if next_retry_at <= now:
                    due_items.append((uid, entry))

            # Limitamos a quantidade por ciclo.
            due_items = due_items[:10]
            for uid, entry in due_items:
                video_id = entry.get('video_id') or ''
                # Tentativa direta: usa remove_video_from_list, mas se falhar
                # vai reaparecer como pendente via _enqueue_pending_removal.
                ok = remove_video_from_list(video_id, uid)
                if ok:
                    _mark_pending_removal_success(uid)
                    print(f"[PENDING_REMOVE] removed pending uniqueId={uid}")
        except Exception as e:
            print(f"[pending_removals_worker] erro ignorado: {e}")
        time.sleep(_PENDING_REMOVALS_WORKER_INTERVAL_SEC)

def remove_video_from_list(video_id, unique_id=None):
    try:
        if not unique_id:
            print("Erro: uniqueId é obrigatório para remover vídeo")
            return False

        # Fazer a requisição para remover o vídeo no servidor
        headers = get_headers()
        response = make_request(
            REMOVE_VIDEO_URL,
            method='post',
            json={'unique_id': unique_id},
            headers=headers,
            timeout=2,
        )
        if response is None:
            # backend fora / circuit breaker: persistir remoção para retentativa
            _enqueue_pending_removal(video_id=video_id, unique_id=unique_id, reason="backend_unavailable")
            return False
        
        if response.status_code in (200, 404):
            # 404 pode acontecer se já removemos parcialmente.
            _mark_pending_removal_success(unique_id)
            print(f"Vídeo com uniqueId: {unique_id} removido com sucesso!")
            return True
        else:
            print(f"Erro ao remover vídeo com uniqueId {unique_id} no servidor: {response.text}")
            _enqueue_pending_removal(video_id=video_id, unique_id=unique_id, reason=f"status_{response.status_code}")
            return False
    except Exception as e:
        print(f"Erro ao remover vídeo com uniqueId {unique_id}: {e}")
        try:
            _enqueue_pending_removal(video_id=video_id, unique_id=unique_id, reason=f"exception:{type(e).__name__}")
        except Exception:
            pass
        return False


# ==== Retomada automática após queda de energia (estado local por máquina) ====
RESUME_STATE_INTERVAL_SEC = 5
# Regra de retomada:
# - se o player já tocou >= 2 minutos antes da queda (position_seconds >= 120),
#   retomamos de onde parou;
# - se tocou < 2 minutos, descartamos esse vídeo e avançamos.
RESUME_MIN_PLAYED_SEC = 120
_resume_state_save_logged = False  # Log uma vez o caminho do ficheiro


def _get_resume_state_path():
    """Caminho do arquivo de estado de retomada (local à máquina, não depende do servidor)."""
    base = os.path.dirname(os.path.abspath(__file__))
    return os.path.join(base, 'playback_resume_state.json')


def _get_current_server_id():
    """server_id do config.ini desta máquina (isolamento por estabelecimento)."""
    try:
        if config and config.has_section('Server'):
            sid = (config.get('Server', 'server_id', fallback='') or '').strip()
            if sid:
                return sid
    except Exception:
        pass
    return 'LOCAL'  # fallback para máquina sem [Server] server_id (estado ainda é salvo)


def _get_config_server_id_raw():
    """server_id do config.ini sem fallback (para validar contra cadastro da API)."""
    try:
        if config and config.has_section('Server'):
            return (config.get('Server', 'server_id', fallback='') or '').strip()
    except Exception:
        pass
    return ''


def _save_queue_cache(items: list) -> None:
    """
    Persiste fila em arquivo após carregamento bem-sucedido via HTTP.
    Falhas de escrita não interrompem a reprodução.
    """
    if not isinstance(items, list):
        return
    try:
        with _QUEUE_CACHE_LOCK:
            _QUEUE_CACHE_FILE.write_text(
                json.dumps(items, ensure_ascii=False),
                encoding="utf-8",
            )
    except Exception as e:
        print(f"[QUEUE_CACHE] falha ao salvar cache local: {e}")


def _load_queue_cache() -> list:
    """
    Lê fila do cache local quando HTTP falha.
    Retorna lista vazia se cache não existe ou está corrompido.
    """
    try:
        with _QUEUE_CACHE_LOCK:
            if not _QUEUE_CACHE_FILE.exists():
                return []
            data = json.loads(_QUEUE_CACHE_FILE.read_text(encoding="utf-8"))
            if isinstance(data, list):
                print(f"[QUEUE_CACHE] fallback local: {len(data)} item(s) do cache")
                return data
    except Exception as e:
        print(f"[QUEUE_CACHE] falha ao ler cache local: {e}")
    return []


def _set_redis_mode(mode: str) -> None:
    normalized = (mode or "").strip().lower()
    if normalized not in ("redis", "fallback", "disabled"):
        normalized = "fallback"
    with _redis_mode_lock:
        global _redis_mode_state
        _redis_mode_state = normalized


def _get_redis_mode() -> str:
    with _redis_mode_lock:
        return _redis_mode_state


def load_resume_state():
    """
    Carrega estado de retomada do arquivo local. Retorna None se inválido ou inexistente.
    Valida server_id para não misturar estados entre máquinas.
    """
    path = _get_resume_state_path()
    if not os.path.isfile(path):
        return None
    try:
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        if not isinstance(data, dict):
            return None
        sid = (data.get('server_id') or '').strip()
        current_sid = _get_current_server_id()
        if sid != current_sid or not current_sid:
            return None
        if not data.get('video_id') or not data.get('unique_id'):
            return None
        pos = data.get('position_seconds', 0)
        dur = data.get('duration_seconds', 0)
        if pos < 0 or dur <= 0 or pos >= dur:
            return None
        return data
    except Exception:
        return None


def save_resume_state(server_id, video_id, unique_id, position_seconds, duration_seconds, video_title=''):
    """Persiste estado a cada 5s durante reprodução. Escrita atômica (tmp + rename)."""
    sid = (server_id or '').strip() or 'LOCAL'
    if not (video_id and unique_id and duration_seconds > 0 and 0 <= position_seconds < duration_seconds):
        return
    path = _get_resume_state_path()
    base_dir = os.path.dirname(path)
    if base_dir and not os.path.isdir(base_dir):
        try:
            os.makedirs(base_dir, exist_ok=True)
        except Exception:
            pass
    tmp = path + '.tmp'
    payload = {
        'server_id': sid,
        'video_id': video_id,
        'unique_id': unique_id,
        'position_seconds': int(position_seconds),
        'duration_seconds': int(duration_seconds),
        'last_updated': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()),
        'video_title': video_title or '',
    }
    try:
        with open(tmp, 'w', encoding='utf-8') as f:
            json.dump(payload, f, ensure_ascii=False, indent=0)
        if os.path.exists(path):
            os.remove(path)
        os.rename(tmp, path)
        global _resume_state_save_logged
        if not _resume_state_save_logged:
            _resume_state_save_logged = True
            print(f"[Retomada] Estado gravado em: {os.path.abspath(path)}")
    except Exception as e:
        if not _resume_state_save_logged:
            print(f"[Retomada] Erro ao gravar estado: {e}")
        try:
            if os.path.isfile(tmp):
                os.remove(tmp)
        except Exception:
            pass


def clear_resume_state():
    """Remove estado de retomada (ao terminar ou pular vídeo)."""
    path = _get_resume_state_path()
    try:
        if os.path.isfile(path):
            os.remove(path)
    except Exception:
        pass


def add_video_to_list(video_id):
    try:
        video_data = {
            'id': {'videoId': video_id},
            'title': '',  # Could be fetched from YouTube if needed
            'thumbnail': '',  # Could be fetched from YouTube if needed
            'uniqueId': f"{video_id}-{int(time.time())}-{secrets.token_hex(4)}"
        }
        
        response = make_request(ADD_VIDEO_URL, method='post', json=video_data)
        if response and response.status_code == 200:
            print(f"Vídeo {video_id} adicionado com sucesso!")
        else:
            print(f"Erro ao adicionar vídeo {video_id}")
    except Exception as e:
        print(f"Erro de conexão ao adicionar vídeo {video_id}: {e}")


def _parse_duration_seconds(v):
    """Extrai duração em segundos de um item da playlist (contentDetails.duration ISO ou duration numérico)."""
    if not isinstance(v, dict):
        return 0
    raw = (v.get('contentDetails') or {}).get('duration') or v.get('duration')
    if isinstance(raw, (int, float)):
        return int(raw)
    if not isinstance(raw, str):
        return 0
    total = 0
    h = re.search(r'(\d+)H', raw)
    m = re.search(r'(\d+)M', raw)
    s = re.search(r'(\d+)S', raw)
    if h:
        total += int(h.group(1)) * 3600
    if m:
        total += int(m.group(1)) * 60
    if s:
        total += int(s.group(1))
    return total


def _format_dur(seconds):
    """Formata segundos em MM:SS ou H:MM:SS."""
    if seconds is None or seconds < 0:
        return '0:00'
    h = seconds // 3600
    m = (seconds % 3600) // 60
    s = seconds % 60
    if h > 0:
        return f'{h}:{m:02d}:{s:02d}'
    return f'{m}:{s:02d}'


def _run_queue_modal():
    """Executa em thread: painel à direita; fecha após 15s de inatividade (ou mesma tecla/ESC)."""
    global playlist, queue_modal_open, close_modal_flag
    queue_modal_open = True
    close_modal_flag = False
    try:
        import html
        import tkinter as tk

        pl = list(playlist) if playlist else []
        items = []
        for i, v in enumerate(pl, 1):
            if not isinstance(v, dict):
                continue
            title = (v.get('snippet') or {}).get('title') or v.get('title') or 'Sem título'
            if isinstance(title, str):
                title = html.unescape(title)  # Corrige &#39; -> ', &amp; -> &, etc.
            sec = _parse_duration_seconds(v)
            items.append((i, title[:80] + ('...' if len(title) > 80 else ''), _format_dur(sec)))

        root = tk.Tk()
        root.withdraw()
        top = tk.Toplevel(root)
        top.overrideredirect(True)  # Sem barra de título
        top.configure(bg='#161616')
        top.attributes('-topmost', True)
        top.attributes('-alpha', 1.0)

        # Posicionar no canto direito da tela
        top.update_idletasks()
        screen_w = top.winfo_screenwidth()
        screen_h = top.winfo_screenheight()
        panel_w = 380
        panel_h = min(640, int(screen_h * 0.85))
        margin = 24
        x = screen_w - panel_w - margin
        y = (screen_h - panel_h) // 2
        top.geometry('%dx%d+%d+%d' % (panel_w, panel_h, x, y))

        # Estilo moderno: container com borda sutil
        container = tk.Frame(top, bg='#161616', highlightbackground='#333', highlightthickness=1)
        container.pack(fill=tk.BOTH, expand=True, padx=1, pady=1)

        # Header compacto
        header = tk.Frame(container, bg='#1e1e1e', height=52)
        header.pack(fill=tk.X)
        header.pack_propagate(0)
        tk.Label(header, text='Fila de vídeos', font=('Segoe UI', 14, 'bold'), fg='#fff', bg='#1e1e1e').pack(side=tk.LEFT, padx=16, pady=14)
        tk.Label(header, text='%d vídeos' % len(items), font=('Segoe UI', 11), fg='#007AFF', bg='#1e1e1e').pack(side=tk.LEFT, padx=0, pady=14)

        # Lista (sem barra de rolagem)
        lb_frame = tk.Frame(container, bg='#161616')
        lb_frame.pack(fill=tk.BOTH, expand=True, padx=12, pady=(0, 12))
        lb = tk.Listbox(
            lb_frame,
            font=('Segoe UI', 11),
            bg='#252525',
            fg='#e0e0e0',
            selectbackground='#007AFF',
            selectforeground='#fff',
            activestyle='none',
            highlightthickness=0,
            borderwidth=0,
            relief=tk.FLAT,
            height=24,
        )
        lb.pack(fill=tk.BOTH, expand=True)
        for (num, title, _) in items:
            lb.insert(tk.END, '  %2d. %s' % (num, title))
        if not items:
            lb.insert(tk.END, '  Nenhum vídeo na fila.')
        else:
            lb.selection_set(0)
            lb.activate(0)

        last_activity = [time.time()]

        def on_activity(*_):
            last_activity[0] = time.time()

        def on_key(event):
            last_activity[0] = time.time()
            if event.keysym == 'Escape':
                globals()['close_modal_flag'] = True

        # Qualquer tecla conta como atividade; ESC além disso fecha
        top.bind('<Key>', on_key)
        lb.bind('<Key>', on_key)
        # Mouse: movimento e clique em qualquer área do painel (incl. labels do header)
        for w in (top, container, header, lb_frame, lb):
            w.bind('<Motion>', on_activity)
            w.bind('<Button>', on_activity)
        for child in header.winfo_children():
            child.bind('<Motion>', on_activity)
            child.bind('<Button>', on_activity)

        top.update()
        last_activity[0] = time.time()
        try:
            top.focus_force()
            lb.focus_set()
        except Exception:
            pass

        INACTIVITY_CLOSE_SEC = 15
        while True:
            try:
                root.update()
            except tk.TclError:
                break
            if close_modal_flag:
                try:
                    top.destroy()
                    root.destroy()
                except Exception:
                    pass
                break
            if time.time() - last_activity[0] >= INACTIVITY_CLOSE_SEC:
                close_modal_flag = True
                continue
            time.sleep(0.25)
    except Exception as e:
        print('Erro ao exibir modal da fila:', e)
    finally:
        queue_modal_open = False


def handle_key_press(player):
    global current_playing_video, playlist, last_played_unique_ids, queue_modal_open, close_modal_flag
    kb_physical = _init_physical_keyboard_listener()
    last_press_time = 0
    key_pressed = False
    
    last_show_list_time = 0
    # Skip admin/app via polling HTTP.
    # Carga estimada:
    # - 1 máquina: 360 req/h em /api/skip_requested (poll a cada 10s)
    # - 1000 máquinas: 360.000 req/h (~100 req/s)
    last_skip_poll_at = 0.0
    SKIP_REQUEST_POLL_FALLBACK_INTERVAL_SEC = 10.0
    while True:
        try:
            with _playlist_lock:
                has_active_video = bool(current_playing_video and playlist)
            # Skip solicitado pelo app (admin em Configurações) — mesmo efeito da tecla skip_video
            now = time.time()
            skip_requested = False
            if has_active_video and skip_requested_socket_event.is_set():
                skip_requested_socket_event.clear()
                skip_requested = get_skip_requested()
            elif has_active_video and (now - last_skip_poll_at) >= SKIP_REQUEST_POLL_FALLBACK_INTERVAL_SEC:
                last_skip_poll_at = now
                skip_requested = get_skip_requested()

            if skip_requested and has_active_video:
                try:
                    with _playlist_lock:
                        current_video_snapshot = current_playing_video
                    if not current_video_snapshot:
                        continue
                    video_id = current_video_snapshot['id']['videoId'] if isinstance(current_video_snapshot['id'], dict) else current_video_snapshot['id']
                    unique_id = current_video_snapshot.get('uniqueId')
                    title = current_video_snapshot.get('title', 'N/A')
                    print(f"\n⏭️ PULANDO VÍDEO (solicitado pelo app - admin):")
                    print(f"ID: {video_id}")
                    print(f"Título: {title}")
                    print(f"UniqueId: {unique_id}")
                    player.skip_current()
                    removed_video = _remove_from_playlist_by_unique_id(unique_id)
                    if removed_video:
                        print(f"✅ Vídeo removido da playlist local: {removed_video.get('title', video_id)}")
                    if unique_id:
                        if 'last_played_unique_ids' not in globals():
                            globals()['last_played_unique_ids'] = set()
                        globals()['last_played_unique_ids'].add(unique_id)
                        if len(globals()['last_played_unique_ids']) > _MAX_LAST_PLAYED:
                            _excess = globals()['last_played_unique_ids']
                            while len(_excess) > int(_MAX_LAST_PLAYED * 0.8):
                                _excess.pop()
                        print(f"📝 Vídeo marcado como já reproduzido (uniqueId: {unique_id})")
                    remove_media_from_list(video_id)
                    if unique_id:
                        threading.Thread(
                            target=remove_video_from_list,
                            args=(video_id, unique_id),
                            daemon=True
                        ).start()
                        print(f"🔄 Removendo vídeo do servidor em background...")
                    print(f"✅ Vídeo pulado com sucesso! Próximo vídeo será reproduzido automaticamente.")
                except Exception as e:
                    print(f"❌ Erro ao pular vídeo (app): {e}")
                    import traceback
                    traceback.print_exc()
            # Tecla para exibir/ocultar lista da fila (modal no PC) — respeita [Controles] allow_show_list
            elif kb_physical and ESTABLISHMENT_CONFIG.get('allow_show_list', True) and keyboard.is_pressed(KEYBOARD_CONFIG.get('show_list', 'l')):
                current_time = time.time()
                if current_time - last_show_list_time > KEYBOARD_CONFIG['debounce_time']:
                    last_show_list_time = current_time
                    if queue_modal_open:
                        close_modal_flag = True
                    else:
                        threading.Thread(target=_run_queue_modal, daemon=True).start()
                    key_pressed = True
            # Tecla para pular vídeo (configurada no config.ini) — respeita [Controles] allow_skip
            elif kb_physical and ESTABLISHMENT_CONFIG.get('allow_skip', True) and keyboard.is_pressed(KEYBOARD_CONFIG['skip_video']):
                current_time = time.time()
                if not key_pressed and current_time - last_press_time > KEYBOARD_CONFIG['debounce_time']:
                    key_pressed = True
                    last_press_time = current_time
                    
                    with _playlist_lock:
                        current_video_snapshot = current_playing_video if (current_playing_video and playlist) else None
                    if current_video_snapshot:
                        try:
                            video_id = current_video_snapshot['id']['videoId'] if isinstance(current_video_snapshot['id'], dict) else current_video_snapshot['id']
                            unique_id = current_video_snapshot.get('uniqueId')
                            title = current_video_snapshot.get('title', 'N/A')
                            
                            print(f"\n⏭️ PULANDO VÍDEO:")
                            print(f"ID: {video_id}")
                            print(f"Título: {title}")
                            print(f"UniqueId: {unique_id}")
                            
                            # Parar reprodução atual
                            player.skip_current()
                            
                            # CRITICAL: Remover o vídeo que está tocando (por uniqueId), não por índice
                            removed_video = _remove_from_playlist_by_unique_id(unique_id)
                            if removed_video:
                                print(f"✅ Vídeo removido da playlist local: {removed_video.get('title', video_id)}")
                            
                            # CRITICAL: Marcar como já reproduzido para evitar duplicatas
                            if unique_id:
                                if 'last_played_unique_ids' not in globals():
                                    globals()['last_played_unique_ids'] = set()
                                globals()['last_played_unique_ids'].add(unique_id)
                                if len(globals()['last_played_unique_ids']) > _MAX_LAST_PLAYED:
                                    _excess = globals()['last_played_unique_ids']
                                    while len(_excess) > int(_MAX_LAST_PLAYED * 0.8):
                                        _excess.pop()
                                print(f"📝 Vídeo marcado como já reproduzido (uniqueId: {unique_id})")
                            
                            # Remover do cache de mídia
                            remove_media_from_list(video_id)
                            
                            # CRITICAL: Remover do servidor imediatamente
                            if unique_id:
                                threading.Thread(
                                    target=remove_video_from_list,
                                    args=(video_id, unique_id),
                                    daemon=True
                                ).start()
                                print(f"🔄 Removendo vídeo do servidor em background...")
                            
                            print(f"✅ Vídeo pulado com sucesso! Próximo vídeo será reproduzido automaticamente.")
                            
                        except Exception as e:
                            print(f"❌ Erro ao pular vídeo: {e}")
                            import traceback
                            traceback.print_exc()
            
            # Tecla configurada para remover vídeo manualmente — respeita [Controles] allow_remove
            elif kb_physical and ESTABLISHMENT_CONFIG.get('allow_remove', True) and keyboard.is_pressed(KEYBOARD_CONFIG['remove_video']):
                current_time = time.time()
                if not key_pressed and current_time - last_press_time > KEYBOARD_CONFIG['debounce_time']:
                    key_pressed = True
                    last_press_time = current_time
                    
                    with _playlist_lock:
                        current_video_snapshot = current_playing_video if (current_playing_video and playlist) else None
                    if current_video_snapshot:
                        try:
                            video_id = current_video_snapshot['id']['videoId']
                            unique_id = current_video_snapshot.get('uniqueId')
                            title = current_video_snapshot.get('title', 'N/A')
                            
                            print(f"\n🗑️ REMOVENDO VÍDEO MANUALMENTE:")
                            print(f"ID: {video_id}")
                            print(f"Título: {title}")
                            print(f"UniqueId: {unique_id}")
                            
                            # Parar reprodução atual
                            player.skip_current()
                            
                            # Remover da playlist por uniqueId (mantém ordem correta)
                            removed_video = _remove_from_playlist_by_unique_id(unique_id)
                            if removed_video:
                                print(f"✅ Vídeo removido da playlist: {removed_video.get('title', video_id)}")
                            remove_media_from_list(video_id)
                            if unique_id:
                                threading.Thread(
                                    target=remove_video_from_list,
                                    args=(video_id, unique_id),
                                    daemon=True
                                ).start()
                                print(f"🔄 Removendo vídeo do servidor em background...")
                            
                            print(f"✅ Vídeo removido com sucesso!")
                            
                        except Exception as e:
                            print(f"❌ Erro ao remover vídeo: {e}")
                    else:
                        print("⚠️ Nenhum vídeo para remover")
            
            else:
                key_pressed = False
                
            time.sleep(0.1)  # Reduzir uso de CPU
            
        except Exception as e:
            print(f"❌ Erro ao processar tecla: {e}")
            time.sleep(0.1)  # Esperar um pouco em caso de erro

def preload_next_video():
    """
    Pré-carrega os próximos itens da fila (até PRELOAD_AHEAD_COUNT) com TTL curto.
    Se não for usado rapidamente, será descartado automaticamente.
    """
    global next_video_url, next_audio_url, next_preload_unique_id, next_preload_expires_at
    try:
        with _playlist_lock:
            playlist_snapshot = list(playlist)
        if len(playlist_snapshot) <= 1:
            print("Não há próximos vídeos para pré-carregar")
            next_preload_unique_id = None
            next_preload_expires_at = None
            return

        ahead = max(1, int(PRELOAD_AHEAD_COUNT or 1))
        max_idx = min(len(playlist_snapshot) - 1, ahead)
        ttl = int(max(180, PRELOAD_STREAM_TTL_SEC))
        preloaded_count = 0

        for idx in range(1, max_idx + 1):
            next_video = playlist_snapshot[idx]
            video_id = next_video['id']['videoId'] if isinstance(next_video.get('id'), dict) else next_video.get('id')
            preload_unique_id = next_video.get('uniqueId')
            if not video_id:
                continue

            # Se já temos preload válido por uniqueId, não repetir.
            if preload_unique_id:
                with preloaded_streams_lock:
                    existing = preloaded_streams_by_unique_id.get(preload_unique_id)
                if isinstance(existing, dict) and existing.get('expires_at') and int(time.time()) < int(existing.get('expires_at')):
                    continue

            stream = get_video_url(video_id, force_refresh=False, min_validity_sec=ttl)
            if not stream:
                continue

            exp_at = int(time.time()) + ttl
            preload_entry = {
                'stream': stream,
                'expires_at': int(exp_at),
                'video_id': video_id,
                'cached_at': int(time.time()),
            }

            if preload_unique_id:
                with preloaded_streams_lock:
                    preloaded_streams_by_unique_id[preload_unique_id] = preload_entry
                    # Manter cache pequeno para evitar crescimento indefinido
                    while len(preloaded_streams_by_unique_id) > 12:
                        first_key = next(iter(preloaded_streams_by_unique_id))
                        preloaded_streams_by_unique_id.pop(first_key, None)

            # Compatibilidade: manter também o slot legado para o próximo imediato (idx=1)
            if idx == 1:
                next_video_url, next_audio_url = stream
                next_preload_unique_id = preload_unique_id
                next_preload_expires_at = int(exp_at)

            preloaded_count += 1

        print(f"[PRELOAD] ttl={ttl} videos_preloaded={preloaded_count}")
    except Exception as e:
        print(f"Erro ao pré-carregar próximo vídeo: {e}")
        next_video_url = None
        next_audio_url = None
        next_preload_unique_id = None
        next_preload_expires_at = None

def monitor_new_videos(player):
    global playlist, current_playing_video, next_video_url, next_audio_url, next_preload_unique_id, next_preload_expires_at, preload_thread, last_played_unique_ids, queue_fetch_last_ok
    
    # Inicializar last_played_unique_ids se não existir
    if 'last_played_unique_ids' not in globals():
        globals()['last_played_unique_ids'] = set()
    
    last_played_unique_ids = globals()['last_played_unique_ids']
    
    queue_fetch_fail_streak = 0
    last_idle_poll_log = None
    last_idle_poll_at = 0.0
    while True:
        _cleanup_failed_videos_cache()

        # Polling inteligente (evita loops agressivos):
        # - Tocando vídeo: atualizar fila raramente (30s) e preferir evento Pub/Sub.
        # - Idle/erro: 60s
        # - Backend em falha/circuit breaker: 90s (prioriza Redis)
        try:
            if player and getattr(player, 'is_active', None) and player.is_active():
                # Com Redis Streams (event-driven) não precisamos atualizar a fila com tanta frequência.
                desired_poll = 180
                poll_state = 'playing'
            else:
                desired_poll = 90 if not _backend_can_try() else 60
                poll_state = 'backend_fail' if not _backend_can_try() else 'idle'
        except Exception:
            desired_poll = 60
            poll_state = 'idle'

        # Só busca lista quando está vazia OU quando evento de fila sinalizou mudança.
        with _playlist_lock:
            playlist_is_empty = not playlist
        if playlist_is_empty:
            now = time.time()
            should_poll = (now - float(last_idle_poll_at or 0.0)) >= float(desired_poll)
            if playlist_refresh_event.is_set():
                should_poll = True
                playlist_refresh_event.clear()
            if should_poll:
                last_idle_poll_at = now
                if last_idle_poll_log != desired_poll:
                    print(f"[POLLING] interval={desired_poll} state={poll_state}")
                    last_idle_poll_log = desired_poll
                update_playlist()
        
        # CRITICAL: Remover vídeos já reproduzidos da playlist para evitar duplicatas
        if last_played_unique_ids:
            with _playlist_lock:
                original_size = len(playlist)
                playlist = [p for p in playlist if p.get('uniqueId') not in last_played_unique_ids]
                removed_count = original_size - len(playlist)
            if removed_count > 0:
                print(f"🗑️ {removed_count} vídeo(s) já reproduzido(s) removido(s) da playlist")
        
        with _playlist_lock:
            has_playlist = bool(playlist)
            if has_playlist:
                current_video = playlist[0]
                current_playing_video = current_video
                queue_size = len(playlist)
            else:
                current_video = None
                queue_size = 0
        if current_video:
            queue_fetch_fail_streak = 0
            player.show_player()
            
            video_id = current_video['id']['videoId'] if isinstance(current_video['id'], dict) else current_video['id']
            unique_id = current_video.get('uniqueId')
            
            print(f"\nIniciando reprodução do vídeo:")
            print(f"ID: {video_id}")
            print(f"Título: {current_video.get('title', 'N/A')}")
            print(f"UniqueId: {unique_id}")
            print(f"Posição na playlist: 1/{queue_size}")
            print(f"[EVENT] PLAYER_DETECTED_VIDEO uniqueId={unique_id} videoId={video_id} queueSize={queue_size} ts={int(time.time()*1000)}")
            log_event("VIDEO_START", {
                "video_id": video_id,
                "unique_id": unique_id,
                "queue_size": queue_size,
                "title": current_video.get('title', 'Sem título')
            })

            recent_failure = _get_recent_failure(video_id)
            if recent_failure:
                reason = recent_failure.get("reason", "recent_failure")
                print(f"[QUEUE] Vídeo {video_id} ignorado temporariamente (cooldown anti-falha): {reason}")
                log_event("VIDEO_SKIPPED", {
                    "video_id": video_id,
                    "unique_id": unique_id,
                    "reason": "recent_failure_cache",
                    "detail": reason
                })
                removed_video = _remove_from_playlist_by_unique_id(unique_id)
                if removed_video and unique_id:
                    last_played_unique_ids.add(unique_id)
                    if len(last_played_unique_ids) > _MAX_LAST_PLAYED:
                        while len(last_played_unique_ids) > int(_MAX_LAST_PLAYED * 0.8):
                            last_played_unique_ids.pop()
                    threading.Thread(
                        target=remove_video_from_list,
                        args=(video_id, unique_id),
                        daemon=True
                    ).start()
                time.sleep(0.2)
                continue
            
            # Retomada automática após queda de energia (estado local)
            resume_start_position = None
            resume_state = load_resume_state()
            current_server_id = _get_current_server_id()
            if resume_state and current_server_id:
                if resume_state.get('unique_id') == unique_id:
                    position = resume_state.get('position_seconds', 0)
                    if position >= RESUME_MIN_PLAYED_SEC:
                        resume_start_position = position
                        print(f"▶️ Retomada após queda de energia: continuando de {resume_start_position}s (tocou >= {RESUME_MIN_PLAYED_SEC}s)")
                    else:
                        # <2 min tocados: excluir vídeo automaticamente e ir para o próximo
                        print(f"▶️ Vídeo com {position}s tocados (<{RESUME_MIN_PLAYED_SEC}s): excluindo e passando ao próximo")
                        clear_resume_state()
                        _remove_from_playlist_by_unique_id(unique_id)
                        if unique_id:
                            last_played_unique_ids.add(unique_id)
                            if len(last_played_unique_ids) > _MAX_LAST_PLAYED:
                                while len(last_played_unique_ids) > int(_MAX_LAST_PLAYED * 0.8):
                                    last_played_unique_ids.pop()
                        threading.Thread(target=remove_video_from_list, args=(video_id, unique_id), daemon=True).start()
                        continue
                else:
                    clear_resume_state()
            
            # Marcador para controlar tentativas de reprodução do mesmo vídeo
            retry_same_video = False
            max_retries_same_video = 3
            current_retry = 0
            
            # Iniciar pré-carregamento do próximo vídeo o mais cedo possível (em paralelo com obter URL do atual)
            if len(playlist) > 1 and (preload_thread is None or not preload_thread.is_alive()):
                preload_thread = threading.Thread(target=preload_next_video, daemon=True)
                preload_thread.start()
            
            while current_retry < max_retries_same_video:
                # Se não for a primeira tentativa, limpar URL em cache e tentar novamente
                if retry_same_video:
                    print(f"Tentativa {current_retry+1}/{max_retries_same_video} para o mesmo vídeo")
                    clear_stream_cache(video_id)
                
                # Reduzir delay entre vídeos: se pré-carregamento está em andamento, esperar até 5s
                # antes de bloquear em get_video_url (yt-dlp pode levar 5-30s)
                if next_video_url is None and preload_thread is not None and preload_thread.is_alive() and not retry_same_video:
                    print("Aguardando pré-carregamento do próximo vídeo (até 5s)...")
                    preload_thread.join(timeout=5.0)
                    if next_video_url is not None:
                        print("Pré-carregamento concluído - usando URL em cache.")
                
                # Primeiro tenta stream pré-carregado por uniqueId (resistente a reordenação da fila)
                print(f"[EVENT] VIDEO_DOWNLOAD_START uniqueId={unique_id} videoId={video_id} ts={int(time.time()*1000)}")
                stream = None
                if not retry_same_video and unique_id:
                    with preloaded_streams_lock:
                        stream = preloaded_streams_by_unique_id.pop(unique_id, None)
                    if isinstance(stream, dict):
                        exp = stream.get('expires_at')
                        if exp and int(time.time()) >= int(exp):
                            stream = None
                            print(f"[QUEUE] Preload por uniqueId expirou ({unique_id}); resolvendo stream fresh.")
                        else:
                            stream = stream.get('stream')
                    if stream:
                        print(f"[QUEUE] Reutilizando stream pré-carregado por uniqueId={unique_id}")

                # Compatibilidade: slot legado de preload (próximo vídeo)
                if stream is None and next_video_url is not None and not retry_same_video and unique_id == next_preload_unique_id:
                    if next_preload_expires_at and int(time.time()) < int(next_preload_expires_at):
                        stream = (next_video_url, next_audio_url)
                        if unique_id:
                            with preloaded_streams_lock:
                                preloaded_streams_by_unique_id.pop(unique_id, None)
                    else:
                        print("[QUEUE] Preload legado expirado; descartando e resolvendo stream fresh.")
                        next_video_url = None
                        next_audio_url = None
                        next_preload_unique_id = None
                        next_preload_expires_at = None
                elif stream is None:
                    if next_video_url is not None and unique_id != next_preload_unique_id:
                        print(f"⚠️ Pré-carregamento era de outro vídeo (fila reordenada); mantendo cache e obtendo URL do atual: {video_id}")
                    stream = resolve_stream(video_id, force_refresh=bool(retry_same_video), min_validity_sec=STREAM_MIN_VALIDITY_SEC)

                # Limpar slot legado apenas quando for consumido pelo uniqueId correspondente
                if unique_id == next_preload_unique_id:
                    next_video_url = None
                    next_audio_url = None
                    next_preload_unique_id = None
                    next_preload_expires_at = None

                if stream:
                    video_url, audio_url = stream
                    try:
                        # Só iniciar nova thread de pré-carregamento se ainda não houver uma em execução
                        # (já podemos ter iniciado no início do bloco quando há 2+ vídeos)
                        if not (preload_thread and preload_thread.is_alive()):
                            preload_thread = threading.Thread(target=preload_next_video, daemon=True)
                            preload_thread.start()

                        player.should_skip = False
                        start_pos = resume_start_position if resume_start_position is not None else None
                        player.play_video(video_url, video_id, audio_url, start_position_seconds=start_pos)
                        print(f"Reproduzindo vídeo {video_id}")
                        print(f"[EVENT] VIDEO_PLAY_START uniqueId={unique_id} videoId={video_id} ts={int(time.time()*1000)}")
                        
                        # Aguarda o carregamento inicial do vídeo com timeout reduzido
                        wait_start = time.time()
                        load_attempts = 0
                        max_load_attempts = 3
                        video_started = False
                        
                        while player.get_length_ms() <= 0:
                            if time.time() - wait_start > 3:  # Timeout reduzido de 5 para 3 segundos
                                load_attempts += 1
                                if load_attempts >= max_load_attempts:
                                    print(f"Timeout ao carregar vídeo após {max_load_attempts} tentativas")
                                    break
                                print(f"Timeout ao carregar vídeo, tentativa {load_attempts}/{max_load_attempts}")
                                
                                # Tentar reiniciar a reprodução
                                player.stop()
                                time.sleep(0.5)
                                player.play_video(video_url, video_id, audio_url)
                                wait_start = time.time()
                            time.sleep(0.1)
                        
                        # Obtém a duração total do vídeo
                        total_duration = player.get_duration_seconds()
                        if total_duration <= 0:
                            print("Erro: Duração inválida do vídeo (possível stream expirado).")
                            player.stop()
                            clear_stream_cache(video_id)
                            _mark_video_failed(video_id, "invalid_duration")
                            if current_retry < max_retries_same_video - 1:
                                current_retry += 1
                                retry_same_video = True
                                print(f"Regenerando URL fresh e tentando novamente ({current_retry+1}/{max_retries_same_video})")
                                time.sleep(0.7)
                                continue
                            else:
                                print("Falha persistente de duração após regenerar URL. Removendo item da fila para evitar loop.")
                                log_event("VIDEO_FAILED", {
                                    "video_id": video_id,
                                    "unique_id": unique_id,
                                    "reason": "invalid_duration_after_retries",
                                    "attempts": max_retries_same_video
                                })
                                removed_video = _remove_from_playlist_by_unique_id(unique_id)
                                if removed_video:
                                    if unique_id:
                                        last_played_unique_ids.add(unique_id)
                                        if len(last_played_unique_ids) > _MAX_LAST_PLAYED:
                                            while len(last_played_unique_ids) > int(_MAX_LAST_PLAYED * 0.8):
                                                last_played_unique_ids.pop()
                                    threading.Thread(
                                        target=remove_video_from_list,
                                        args=(video_id, unique_id),
                                        daemon=True
                                    ).start()
                                break
                        else:
                            print(f"Duração total: {total_duration} segundos")
                            video_started = True
                            _clear_video_failure(video_id)
                            log_event("VIDEO_PLAY_SUCCESS", {
                                "video_id": video_id,
                                "unique_id": unique_id,
                                "duration_seconds": total_duration
                            })
                        
                        # Verificar se o player iniciou corretamente
                        try:
                            time.sleep(0.2)  # espera mínima para o player sinalizar (evita delay entre vídeos)
                            if player.is_active():
                                print("✅ Player está reproduzindo")
                            else:
                                print("⚠️ Player não sinalizou reprodução ativa")
                        except Exception as e:
                            print(f"⚠️ Erro ao verificar estado do player: {e}")
                        
                        # Se o vídeo não iniciou corretamente e temos mais tentativas, tente novamente
                        if not video_started and current_retry < max_retries_same_video - 1:
                            current_retry += 1
                            retry_same_video = True
                            player.stop()
                            print(f"Vídeo não iniciou corretamente, tentando novamente ({current_retry+1}/{max_retries_same_video})")
                            continue
                        
                        # Monitora o progresso do vídeo
                        last_position = -1
                        stall_count = 0
                        stall_position = -1
                        last_update_time = time.time()
                        last_resume_save_time = time.time()  # Estado de retomada a cada 5s
                        # Inicializa estado do envio inteligente de progresso
                        global last_progress_sent_ts, last_progress_sent_time
                        last_progress_sent_ts = time.time()
                        last_progress_sent_time = int(last_position) if last_position is not None else -1
                        
                        # Para detecção de travamento no início do vídeo
                        initial_stall_time = time.time()
                        initial_grace_period = 30  # Tempo de carregamento inicial em segundos
                        
                        # Iniciar remoção do vídeo em background quando estiver próximo do fim
                        removal_started = False
                        preload_triggered = False  # Pré-buffer do próximo vídeo aos 5s do fim
                        
                        # Flag para detecção de erro de reprodução
                        player_error_detected = False
                        error_start_time = None
                        
                        # Verificação de playlist durante reprodução:
                        # - resync raro por tempo (fallback contra perda de eventos)
                        # - atualização imediata por evento de fila (Socket.IO)
                        last_playlist_check = time.time()
                        playlist_check_interval = 180
                        print(f"[RESYNC] interval={playlist_check_interval}s state=playing")
                        
                        while player.is_active() and not player.should_skip:
                            try:
                                current_time = player.get_position_seconds()
                                current_real_time = time.time()
                                
                                # Verificar novos vídeos por timeout ou evento (quase em tempo real)
                                if playlist_refresh_event.is_set() or (current_real_time - last_playlist_check >= playlist_check_interval):
                                    last_playlist_check = current_real_time
                                    if playlist_refresh_event.is_set():
                                        playlist_refresh_event.clear()
                                    # CRITICAL: Usar update_playlist() para manter sincronização e ordem correta
                                    # Isso garante que vídeos removidos sejam removidos da playlist local também
                                    try:
                                        # Salvar o vídeo atual antes de atualizar
                                        current_video_before_update = current_playing_video
                                        
                                        # Atualizar playlist (já remove vídeos que não estão mais no servidor)
                                        update_playlist()
                                        
                                        # Verificar se o vídeo atual ainda está na playlist
                                        if current_video_before_update:
                                            current_unique_id = current_video_before_update.get('uniqueId')
                                            if current_unique_id and not any(p.get('uniqueId') == current_unique_id for p in playlist):
                                                print(f"⚠️ Vídeo atual foi removido do servidor durante reprodução")
                                    except Exception as e:
                                        print(f"⚠️ Erro ao verificar novos vídeos durante reprodução: {e}")
                                
                                # Verificar erros do player
                                if player.has_error():
                                    if not player_error_detected:
                                        player_error_detected = True
                                        error_start_time = time.time()
                                        print("Erro de reprodução detectado: VLC retornou estado de erro")
                                    elif time.time() - error_start_time > 5:  # 5 segundos tentando recuperar
                                        print("Não foi possível recuperar do erro de reprodução")
                                        break
                                else:
                                    player_error_detected = False
                                
                                # Verifica se o vídeo está travado
                                if current_time == stall_position:
                                    # Se estamos no início do vídeo (primeiros 5 segundos), dar mais tempo para carregar
                                    if current_time <= 5 and time.time() - initial_stall_time < initial_grace_period:
                                        # Reinicia o contador de tempo real para dar mais tempo de carregamento
                                        if stall_count % 20 == 0:  # Log a cada 2 segundos aproximadamente
                                            print(f"Aguardando carregamento inicial: {int(time.time() - initial_stall_time)}/{initial_grace_period} segundos")
                                        stall_count += 1
                                    else:
                                        stall_count += 1
                                        # Ser mais tolerante com travamentos (30 segundos = ~300 verificações)
                                        if stall_count > 300:
                                            print("Vídeo travado, avançando...")
                                            break
                                        # Tentar recuperar o vídeo a cada 100 verificações (~10 segundos)
                                        elif stall_count > 0 and stall_count % 100 == 0:
                                            print(f"Tentando recuperar vídeo travado (tentativa {stall_count//100})...")
                                            # Pausar e continuar para tentar destravar
                                            player.pause()
                                            time.sleep(0.5)
                                            player.resume()
                                        elif stall_count % 50 == 0:  # Log a cada 5 segundos aprox
                                            print(f"Possível travamento detectado: {stall_count}/300")
                                else:
                                    stall_count = 0
                                    stall_position = current_time
                                
                                # Atualiza progresso com modelo inteligente (tempo + eventos)
                                now = current_real_time
                                # Evitar "burst" inicial:
                                # se ainda não enviamos nada (last_progress_sent_time < 0), inicializa com o tempo atual
                                # e só passa a considerar envio na próxima iteração.
                                if last_progress_sent_time is None or int(last_progress_sent_time) < 0:
                                    last_progress_sent_ts = now
                                    try:
                                        last_progress_sent_time = int(current_time)
                                    except Exception:
                                        last_progress_sent_time = -1
                                    last_position = current_time
                                    continue

                                time_elapsed = float(now) - float(last_progress_sent_ts or 0.0)
                                try:
                                    time_diff = abs(int(current_time) - int(last_progress_sent_time))
                                except Exception:
                                    time_diff = 999999

                                should_send = False
                                # Quase realtime: envia a cada ~2s enquanto reproduz,
                                # mas ainda garante sincronização imediata em seek e final do vídeo.
                                if time_elapsed >= 2.0:
                                    should_send = True
                                elif time_diff >= 10:
                                    should_send = True
                                elif total_duration > 0 and current_time >= total_duration - 2:
                                    should_send = True

                                if should_send and current_time != last_position:
                                    update_video_progress(
                                        video_id=video_id,
                                        current_time=current_time,
                                        duration=total_duration,
                                        unique_id=unique_id
                                    )
                                    print(f"Progresso: {current_time}/{total_duration}")
                                    # Debug leve
                                    print(f"[PROGRESS_SEND] sent={should_send} elapsed={time_elapsed:.1f}s diff={time_diff}")
                                    last_position = current_time
                                    last_update_time = now
                                    last_progress_sent_ts = now
                                    last_progress_sent_time = int(current_time)
                                # Persistir estado de retomada a cada 5s (queda de energia)
                                if current_real_time - last_resume_save_time >= RESUME_STATE_INTERVAL_SEC:
                                    if total_duration > 0 and 0 <= current_time < total_duration:
                                        save_resume_state(
                                            _get_current_server_id(),
                                            video_id,
                                            unique_id,
                                            current_time,
                                            total_duration,
                                            current_video.get('title', '')
                                        )
                                    last_resume_save_time = current_real_time
                                
                                # Reforçar pré-carregamento do próximo quando faltarem 5s para o fim (se ainda não concluído)
                                if not preload_triggered and current_time >= total_duration - 5:
                                    preload_triggered = True
                                    if not (preload_thread and preload_thread.is_alive()) and next_video_url is None:
                                        preload_thread = threading.Thread(target=preload_next_video, daemon=True)
                                        preload_thread.start()
                                
                                # Iniciar remoção do vídeo em background quando estiver próximo do fim
                                if not removal_started and current_time >= total_duration - 3:  # 3 segundos antes do fim
                                    removal_started = True
                                    threading.Thread(
                                        target=remove_video_from_list,
                                        args=(video_id, unique_id),
                                        daemon=True
                                    ).start()
                                
                                # Verifica se o vídeo terminou
                                if current_time >= total_duration:
                                    print("Vídeo finalizado normalmente")
                                    break
                                
                                if player.get_state() in (PlayerState.STOPPED, PlayerState.ENDED):
                                    print("Reprodução interrompida")
                                    break
                                
                            except Exception as e:
                                print(f"Erro ao atualizar progresso: {e}")
                                time.sleep(0.1)
                                continue
                            
                            time.sleep(0.1)  # Reduced sleep time for smoother updates

                        if player.should_skip:
                            print("Vídeo pulado pelo usuário")
                        
                        # Limpa o player e estado de retomada (vídeo terminou ou foi pulado)
                        player.stop()
                        clear_resume_state()
                        
                        print(f"\nFinalizando vídeo {video_id}")
                        
                        # Remove o vídeo que realmente tocou (por uniqueId), não por índice — evita troca de ordem após update_playlist()
                        removed_video = _remove_from_playlist_by_unique_id(unique_id)
                        if removed_video:
                            remove_media_from_list(video_id)
                            if unique_id:
                                last_played_unique_ids.add(unique_id)
                                if len(last_played_unique_ids) > _MAX_LAST_PLAYED:
                                    while len(last_played_unique_ids) > int(_MAX_LAST_PLAYED * 0.8):
                                        last_played_unique_ids.pop()
                                print(f"📝 Vídeo marcado como já reproduzido (uniqueId: {unique_id})")
                            if not removal_started:
                                threading.Thread(
                                    target=remove_video_from_list,
                                    args=(video_id, unique_id),
                                    daemon=True
                                ).start()
                            if playlist:
                                print(f"Próximo vídeo: {playlist[0].get('title', 'N/A')}")
                            else:
                                print("Playlist vazia - aguardando novos videos")
                        else:
                            print("Vídeo já havia sido removido da playlist ou uniqueId ausente")
                        
                        # Vídeo foi reproduzido, saia do loop de tentativas
                        break
                        
                    except Exception as e:
                        print(f"Erro durante reprodução do vídeo: {e}")
                        clear_stream_cache(video_id)
                        _mark_video_failed(video_id, f"play_exception:{type(e).__name__}")
                        # Se ainda temos mais tentativas, tente novamente
                        if current_retry < max_retries_same_video - 1:
                            current_retry += 1
                            retry_same_video = True
                            player.stop()
                            print(f"Erro ao reproduzir; limpando cache e regenerando URL ({current_retry+1}/{max_retries_same_video})")
                            continue
                        else:
                            # Mesmo com erro, continuar removendo o vídeo que tentamos (por uniqueId)
                            print(f"Video {video_id} falhou apos {max_retries_same_video} tentativas, removendo da playlist")
                            log_event("VIDEO_FAILED", {
                                "video_id": video_id,
                                "unique_id": unique_id,
                                "reason": f"play_exception:{type(e).__name__}",
                                "attempts": max_retries_same_video
                            })
                            removed_video = _remove_from_playlist_by_unique_id(unique_id)
                            if removed_video:
                                remove_media_from_list(video_id)
                                if unique_id:
                                    last_played_unique_ids.add(unique_id)
                                    if len(last_played_unique_ids) > _MAX_LAST_PLAYED:
                                        while len(last_played_unique_ids) > int(_MAX_LAST_PLAYED * 0.8):
                                            last_played_unique_ids.pop()
                                    print(f"📝 Vídeo marcado como já reproduzido (mesmo com erro) (uniqueId: {unique_id})")
                                threading.Thread(
                                    target=remove_video_from_list,
                                    args=(video_id, unique_id),
                                    daemon=True
                                ).start()
                            else:
                                print("Playlist ja estava vazia ou vídeo já removido")
                            break
                else:
                    print(f"Não foi possível obter URL do vídeo {video_id}")
                    clear_stream_cache(video_id)
                    _mark_video_failed(video_id, "stream_resolve_failed")
                    # Se ainda temos mais tentativas, tente novamente
                    if current_retry < max_retries_same_video - 1:
                        current_retry += 1
                        retry_same_video = True
                        print(f"Tentando resolver stream fresh novamente ({current_retry+1}/{max_retries_same_video})")
                        # Esperar um pouco antes de tentar novamente
                        time.sleep(1)  # Reduzido de 2 para 1 segundo
                        continue
                    else:
                        # Aguardar um pouco antes de remover o vídeo da playlist
                        print(f"Tentando novamente em 2 segundos...")  # Reduzido de 3 para 2
                        time.sleep(2)  # Reduzido de 3 para 2 segundos
                        log_event("VIDEO_FAILED", {
                            "video_id": video_id,
                            "unique_id": unique_id,
                            "reason": "stream_resolve_failed",
                            "attempts": max_retries_same_video
                        })
                        # Se após o tempo de espera ainda não conseguiu stream para este uniqueId, remover da playlist
                        with preloaded_streams_lock:
                            preload_entry = preloaded_streams_by_unique_id.get(unique_id) if unique_id else None
                            if isinstance(preload_entry, dict):
                                exp = preload_entry.get('expires_at')
                                has_unique_preload = bool(exp and int(time.time()) < int(exp))
                            else:
                                has_unique_preload = False
                        if not has_unique_preload:
                            removed_video = _remove_from_playlist_by_unique_id(unique_id)
                            if removed_video:
                                if unique_id:
                                    last_played_unique_ids.add(unique_id)
                                    if len(last_played_unique_ids) > _MAX_LAST_PLAYED:
                                        while len(last_played_unique_ids) > int(_MAX_LAST_PLAYED * 0.8):
                                            last_played_unique_ids.pop()
                                    print(f"📝 Vídeo marcado como já reproduzido (sem URL) (uniqueId: {unique_id})")
                                threading.Thread(
                                    target=remove_video_from_list,
                                    args=(video_id, unique_id),
                                    daemon=True
                                ).start()
                            else:
                                print("Playlist ja estava vazia")
                        else:
                            print(f"[QUEUE] Stream apareceu em preload para uniqueId={unique_id}; mantendo item para próxima tentativa")
                        break
        else:
            print("Aguardando novos vídeos...")
            # Debug: mostrar estado atual
            print(f"🔍 Estado atual - Playlist: {len(playlist) if playlist else 0} vídeos, Player: {player.get_state()}")
            
            # Só esconder o player se não houver vídeos na playlist E não estiver reproduzindo
            if not playlist and not player.is_active():
                print("Playlist vazia - player indo para segundo plano")
                player.hide_player()
            else:
                print("Aguardando proximo video - player permanece visivel")
            if queue_fetch_last_ok:
                queue_fetch_fail_streak = 0
                time.sleep(1)
            else:
                queue_fetch_fail_streak = min(queue_fetch_fail_streak + 1, 6)
                backoff = min(6, 1 + queue_fetch_fail_streak)
                print(f"[QUEUE] Falha ao carregar fila (streak={queue_fetch_fail_streak}). Backoff: {backoff}s")
                time.sleep(backoff)

def check_server_status():
    """Retorna (ok: bool, detalhe: str). Falha em timeout/rede não encerra imediatamente."""
    try:
        health_url = f'{BASE_URL}/health'
        if config and config.has_section('Server'):
            sid = (config.get('Server', 'server_id', fallback='') or '').strip()
            if sid:
                health_url = f'{BASE_URL}/health?server_id={sid}'
        try:
            total = max(5, min(120, int(HEALTH_CHECK_TIMEOUT_SEC or 25)))
        except Exception:
            total = 25
        # TCP/TLS separado do corpo da resposta: falha rápida se host morto; leitura pode ser mais longa em CDN.
        connect_to = min(15, max(4, total // 2))
        read_to = max(8, total - connect_to)
        response = requests.get(
            health_url,
            headers=REQUEST_HEADERS_CONNECTION_CLOSE,
            timeout=(connect_to, read_to),
            verify=False,
            proxies=REQUESTS_NO_PROXY,
        )
        if response.status_code == 200:
            return True, 'ok'
        return False, f'HTTP {response.status_code}'
    except requests.exceptions.Timeout:
        return False, 'timeout'
    except requests.exceptions.ConnectionError as e:
        return False, f'conexão: {type(e).__name__}'
    except Exception as e:
        return False, f'{type(e).__name__}: {e}'

def monitor_server_status(player):
    # Modo resiliente: em rede remota instável, não matar o player por indisponibilidade temporária do backend.
    # O player continua rodando e tenta reconectar periodicamente.
    consecutive_health_failures = 0
    max_consecutive_health_failures = 24
    health_check_interval = 5
    last_error_time = None
    consecutive_errors = 0
    max_consecutive_errors = 3
    try:
        if config and config.has_section('Player'):
            health_check_interval = max(
                3,
                int(config.get('Player', 'health_check_interval_seconds', fallback=str(health_check_interval)))
            )
            max_consecutive_health_failures = max(
                6,
                int(config.get('Player', 'max_consecutive_health_failures', fallback=str(max_consecutive_health_failures)))
            )
    except Exception:
        pass
    
    while True:
        try:
            # Verifica se o servidor está online (com tolerância a falhas transitórias)
            ok, detail = check_server_status()
            if ok:
                if consecutive_health_failures > 0:
                    print(f"\n✅ Servidor voltou a responder após {consecutive_health_failures} falha(s).")
                consecutive_health_failures = 0
            else:
                consecutive_health_failures = min(
                    consecutive_health_failures + 1,
                    max_consecutive_health_failures,
                )
                try:
                    _ht = int(HEALTH_CHECK_TIMEOUT_SEC or 25)
                except Exception:
                    _ht = 25
                print(
                    f"\n⚠️ Health check falhou ({consecutive_health_failures}/{max_consecutive_health_failures}): "
                    f"{detail} — BASE_URL={BASE_URL}"
                )
                _log_connection_failure(Exception(detail), stage='health', extra={'timeout': _ht})
                if consecutive_health_failures >= max_consecutive_health_failures:
                    print(
                        f"\n❌ Servidor offline persistente após "
                        f"{max_consecutive_health_failures} falhas consecutivas. "
                        "Mantendo player ativo e tentando reconectar (modo resiliente)."
                    )
            
            # Verifica se o player está travado
            if player.has_error():
                current_time = time.time()
                if last_error_time is None:
                    last_error_time = current_time
                    consecutive_errors = 1
                elif current_time - last_error_time < 30:  # Erros dentro de 30 segundos
                    consecutive_errors += 1
                    if consecutive_errors >= max_consecutive_errors:
                        print(f"\n{max_consecutive_errors} erros consecutivos detectados. Reiniciando player...")
                        # Reiniciar player
                        player.stop()
                        time.sleep(1)
                        if current_playing_video:
                            video_id = current_playing_video['id']['videoId'] if isinstance(current_playing_video['id'], dict) else current_playing_video['id']
                            stream = get_video_url(video_id)
                            if stream:
                                video_url, audio_url = stream
                                player.play_video(video_url, video_id, audio_url)
                        consecutive_errors = 0
                else:
                    # Reseta contador se o último erro foi há mais de 30 segundos
                    last_error_time = current_time
                    consecutive_errors = 1
            else:
                last_error_time = None
                consecutive_errors = 0
            
            time.sleep(health_check_interval)
        except Exception as e:
            print(f"Erro ao monitorar servidor: {e}")
            time.sleep(health_check_interval)

HEARTBEAT_INTERVAL_SEC = 10  # Renovar heartbeat HTTP a cada 10s
# Timeout da requisição (read): MySQL lento no central ou rede ruim — pode subir via env (ex.: 30).
HEARTBEAT_HTTP_TIMEOUT_SEC = int(os.environ.get("HEARTBEAT_HTTP_TIMEOUT_SEC", "25"))


def _player_heartbeat_loop():
    """
    Loop em background: envia POST /player/heartbeat periodicamente.
    O backend é o único responsável por persistir heartbeat em Redis/MySQL.
    """
    time.sleep(2)  # Primeiro envio após 2s para não sobrecarregar no startup
    consecutive_failures = 0
    log_every_n_failures = 6  # Logar a cada ~1 min (6 x 10s) para não poluir
    last_diag_log_monotonic = 0.0
    while True:
        http_ok = False
        try:
            headers = get_headers()
            if headers and BASE_URL:
                r = requests.post(
                    f'{BASE_URL}/player/heartbeat',
                    json={'redis_mode': _get_redis_mode()},
                    headers=headers,
                    timeout=max(10, HEARTBEAT_HTTP_TIMEOUT_SEC),
                    verify=False,
                    proxies=REQUESTS_NO_PROXY,
                )
                if r.status_code == 200:
                    http_ok = True
                    consecutive_failures = 0
                elif r.status_code == 401:
                    # JWT expirou (ex.: API Key com validade de dias) — renovar e religar Socket.IO com o token novo.
                    print("[heartbeat] HTTP 401 — token expirado ou inválido; renovando autenticação...")
                    if authenticate():
                        try:
                            restart_edge_progress_socketio()
                        except Exception as _re_sio:
                            print(f"[heartbeat] Falha ao reiniciar Socket.IO após reauth: {_re_sio}")
                        consecutive_failures = 0
                    else:
                        consecutive_failures += 1
                        if consecutive_failures % log_every_n_failures == 1:
                            print(
                                f"[heartbeat] Reautenticação falhou após 401. "
                                f"Verifique API Key / rede. BASE_URL={BASE_URL}"
                            )
                else:
                    consecutive_failures += 1
                    if consecutive_failures % log_every_n_failures == 1:
                        print(
                            f"[heartbeat] Falha HTTP {r.status_code} ao enviar heartbeat para {BASE_URL}. "
                            "Maquina pode aparecer offline. Verifique URL, API Key e rede."
                        )
            else:
                consecutive_failures += 1
                if consecutive_failures == 1:
                    print(
                        "[heartbeat] Sem headers ou BASE_URL; heartbeat HTTP nao enviado. "
                        "Verifique config.ini e autenticacao."
                    )
            if http_ok:
                now_mono = time.monotonic()
                if (now_mono - last_diag_log_monotonic) >= 60:
                    print(
                        "[heartbeat] ok "
                        f"server_id={(_get_current_server_id() or '').strip() or 'UNKNOWN'} "
                        f"http=ok "
                        f"base_url={_redact_url_for_log(BASE_URL)}"
                    )
                    last_diag_log_monotonic = now_mono
        except Exception as e:
            consecutive_failures += 1
            if consecutive_failures % log_every_n_failures == 1:
                print(f"[heartbeat] Erro ao enviar heartbeat: {e}. URL={BASE_URL}. App pode mostrar 'maquina offline'.")
        time.sleep(HEARTBEAT_INTERVAL_SEC)


def _maybe_discard_first_video_on_startup():
    """
    Se config [shutdownvideo] option=1, descarta o primeiro vídeo da fila no backend ao iniciar.
    Assim o vídeo que estava tocando antes do shutdown da máquina não fica na fila.
    """
    try:
        if not config or not config.has_section('shutdownvideo'):
            return
        opt = (config.get('shutdownvideo', 'option', fallback='0') or '0').strip().lower()
        if opt not in ('1', 'on', 'true', 'yes'):
            return
        url = f'{BASE_URL}/discard_first_video_on_startup'
        headers = get_headers()
        if not headers:
            return
        r = requests.post(url, headers=headers, timeout=15, verify=False, proxies=REQUESTS_NO_PROXY)
        if r.status_code == 200:
            data = r.json() if r.text else {}
            if data.get('discarded'):
                print("[shutdownvideo] Primeiro video da fila descartado ao iniciar (option=1).")
            else:
                print("[shutdownvideo] Fila vazia ou nenhum video descartado.")
        else:
            print(f"[shutdownvideo] Nao foi possivel descartar primeiro video: HTTP {r.status_code}")
    except Exception as e:
        print(f"[shutdownvideo] Erro ao descartar primeiro video: {e}")


def _start_ytdlp_auto_update_if_enabled():
    """Inicia atualização automática do yt-dlp em background (jukebox no bar). Não bloqueia.
    ytdlp_updater.py deve estar na MESMA pasta do play.py em cada máquina (MAQUINA_1, MAQUINA_2, ...).
    """
    try:
        import importlib.util
        backend_dir = os.path.dirname(os.path.abspath(__file__))
        updater_path = os.path.join(backend_dir, "ytdlp_updater.py")
        if not os.path.isfile(updater_path):
            raise ImportError(f"ytdlp_updater.py não encontrado em {backend_dir}")
        spec = importlib.util.spec_from_file_location("ytdlp_updater", updater_path)
        if spec is None or spec.loader is None:
            raise ImportError("ytdlp_updater: spec inválido")
        ytdlp_updater = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(ytdlp_updater)
        delay_sec = 15.0
        skip_recent = 3600  # 1h
        enabled = True
        if config is not None and config.has_section('DependencyManager'):
            try:
                enabled = config.getboolean('DependencyManager', 'ytdlp_auto_update_enabled', fallback=True)
                delay_sec = config.getfloat('DependencyManager', 'ytdlp_update_delay_seconds', fallback=15.0)
                skip_recent = config.getint('DependencyManager', 'ytdlp_skip_if_updated_recently_seconds', fallback=3600)
            except Exception:
                pass
        if enabled:
            ytdlp_updater.run_ytdlp_update_in_background(
                config_dir=backend_dir,
                delay_seconds=delay_sec,
                update_timeout=120,
                skip_if_updated_recently_seconds=skip_recent,
                daemon=True,
            )
    except ImportError as e:
        print(f"[yt-dlp] Auto-update não iniciado: {e}. Copie ytdlp_updater.py para esta máquina (mesma pasta do play.py).")
    except Exception as e:
        print(f"[yt-dlp] Auto-update não iniciado: {e}")


def play_videos():
    if _needs_provisioning():
        ok = _run_provisioning_bootstrap()
        if not ok:
            print("[PROVISION] Provisionamento automático falhou. Configure api_key manualmente no config.ini.")

    if not authenticate():
        print("")
        print("Falha na autenticação automática. O programa será encerrado.")
        print("Causas frequentes: backend central offline, túnel desligado, firewall, ou API Key/URL incorretos.")
        print("")
        sys.exit(1)

    # [shutdownvideo] option=1: descartar primeiro vídeo da fila ao iniciar (estava tocando antes do shutdown)
    _maybe_discard_first_video_on_startup()

    # Diagnóstico operacional: ajuda a validar se player aponta para backend/config corretos.
    _log_player_routing_diagnostics()

    # Progresso quase realtime: conecta Socket.IO para enviar eventos ao backend
    init_edge_progress_socketio()

    # Atualização do yt-dlp em background (não bloqueia reprodução)
    _start_ytdlp_auto_update_if_enabled()

    config = load_config()
    player, player_engine_name = select_player(config)
    print(f"▶ Engine do player: {player_engine_name}")

    # Criar thread para monitorar vídeos
    monitor_thread = threading.Thread(target=monitor_new_videos, args=(player,), daemon=True)
    monitor_thread.start()

    # Redis no player removido (Etapa 3): fila via HTTP + cache local.
    _set_redis_mode("disabled")

    # Criar thread para monitorar teclas
    keyboard_thread = threading.Thread(target=handle_key_press, args=(player,), daemon=True)
    keyboard_thread.start()

    # Criar thread para monitorar status do servidor
    server_monitor_thread = threading.Thread(target=monitor_server_status, args=(player,), daemon=True)
    server_monitor_thread.start()

    # Remoções pendentes: garante que vídeos já reproduzidos não "renascem"
    # após circuit breaker/timeouts e reinícios.
    pending_removals_thread = threading.Thread(target=pending_removals_worker, daemon=True)
    pending_removals_thread.start()

    # Heartbeat periódico: atualiza last_online no backend para status ONLINE e permite adicionar vídeos
    heartbeat_thread = threading.Thread(target=_player_heartbeat_loop, daemon=True)
    heartbeat_thread.start()

    # Servidor de volume na máquina do bar (SaaS: central faz ponte, play.py executa o volume)
    volume_server_thread = threading.Thread(target=_run_volume_server, daemon=True)
    volume_server_thread.start()

    # Manter a aplicação viva enquanto as threads rodam
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("Encerrando reprodução por interrupção do usuário...")
        _logout_player_on_exit()  # Marcar servidor offline no backend logo ao Ctrl+C
        player.stop()

if __name__ == '__main__':
    play_videos()
