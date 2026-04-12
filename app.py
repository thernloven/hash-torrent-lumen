import os
import re
import logging
import threading
import time

import requests
import libtorrent as lt
from flask import Flask, jsonify, request
from flask_cors import CORS
from functools import wraps

app = Flask(__name__)
CORS(app)
app.config['JSONIFY_PRETTYPRINT_REGULAR'] = True

# Logging — ensures output shows in gunicorn error log
gunicorn_logger = logging.getLogger('gunicorn.error')
app.logger.handlers = gunicorn_logger.handlers
app.logger.setLevel(gunicorn_logger.level)
log = app.logger

# Config
API_KEY = os.getenv('API_KEY', 'change-me-in-production')
BACKEND_URL = os.getenv('BACKEND_URL', 'http://localhost:3000')
DOWNLOAD_PATH = os.getenv('DOWNLOAD_PATH', '/tmp/torrents')
IDLE_SHUTDOWN_MINUTES = int(os.getenv('IDLE_SHUTDOWN_MINUTES', '10'))

os.makedirs(DOWNLOAD_PATH, exist_ok=True)

# Libtorrent session
ses = lt.session({
    'listen_interfaces': '0.0.0.0:6881,[::]:6881',
    'alert_mask': lt.alert.category_t.all_categories,
    'enable_dht': True,
    'enable_lsd': True,
    'enable_pex': True,
})

# Bootstrap DHT with well-known nodes so metadata resolves on fresh workers
DHT_BOOTSTRAP_NODES = [
    ("router.bittorrent.com", 6881),
    ("router.utorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("dht.aelitis.com", 6881),
]
for host, port in DHT_BOOTSTRAP_NODES:
    ses.add_dht_node((host, port))
log.info(f"[DHT] Bootstrapped with {len(DHT_BOOTSTRAP_NODES)} nodes")

# Track torrents: info_hash -> {handle, r2_key, content_id, status, upload_progress}
active_torrents = {}
last_activity = time.time()

# -------------------------------------------------------------------
# Auth
# -------------------------------------------------------------------

def require_auth(f):
    @wraps(f)
    def decorated(*args, **kwargs):
        key = request.headers.get('X-API-Key')
        if key != API_KEY:
            return jsonify({'error': 'Unauthorized'}), 401
        return f(*args, **kwargs)
    return decorated

# -------------------------------------------------------------------
# Health
# -------------------------------------------------------------------

@app.route('/health', methods=['GET'])
def health():
    return jsonify({'status': 'ok', 'active_torrents': len(active_torrents)}), 200


@app.route('/status', methods=['GET'])
@require_auth
def status():
    '''Report worker capacity: active torrents and free disk space.'''
    import shutil
    disk = shutil.disk_usage(DOWNLOAD_PATH)
    downloading = sum(1 for t in active_torrents.values() if t['status'] == 'downloading')
    uploading = sum(1 for t in active_torrents.values() if t['status'] in ('uploading', 'uploading_season'))
    return jsonify({
        'active_torrents': len(active_torrents),
        'downloading': downloading,
        'uploading': uploading,
        'disk_total_gb': round(disk.total / (1024 ** 3), 1),
        'disk_free_gb': round(disk.free / (1024 ** 3), 1),
    }), 200

# -------------------------------------------------------------------
# Torrent management
# -------------------------------------------------------------------

@app.route('/torrents/add', methods=['POST'])
@require_auth
def add_torrent():
    data = request.json or {}
    magnet = data.get('magnet')
    if not magnet:
        return jsonify({'error': 'No magnet link provided'}), 400

    log.info(f'[ADD] Adding torrent, content_id={data.get("content_id")}, has_r2_key={bool(data.get("r2_key"))}')

    params = lt.parse_magnet_uri(magnet)
    params.save_path = DOWNLOAD_PATH
    handle = ses.add_torrent(params)
    info_hash = str(handle.info_hash())
    log.info(f'[ADD] Torrent added: hash={info_hash}')

    active_torrents[info_hash] = {
        'handle': handle,
        'r2_key': data.get('r2_key'),
        'content_id': data.get('content_id'),
        'callback_url': data.get('callback_url'),
        'season_pack': data.get('season_pack', False),
        'status': 'downloading',
        'upload_progress': 0,
        'upload_total_files': 0,
        'upload_current_file': 0,
        'added_at': time.time(),
    }

    global last_activity
    last_activity = time.time()

    return jsonify({'status': 'ok', 'hash': info_hash}), 200


@app.route('/torrents', methods=['GET'])
@require_auth
def list_torrents():
    result = []
    for info_hash, t in list(active_torrents.items()):
        handle = t['handle']
        s = handle.status()

        eta = -1
        if s.download_rate > 0 and s.total_wanted > 0:
            remaining = s.total_wanted - s.total_wanted_done
            eta = int(remaining / s.download_rate)

        state_map = {
            0: 'queued', 1: 'checking', 2: 'downloading_metadata',
            3: 'downloading', 4: 'finished', 5: 'seeding',
            6: 'allocating', 7: 'checking_resume',
        }

        result.append({
            'hash': info_hash,
            'name': s.name or 'Fetching metadata...',
            'size': s.total_wanted,
            'progress': round(s.progress * 100, 1),
            'dlspeed': s.download_rate,
            'upspeed': s.upload_rate,
            'state': t['status'] if t['status'] in ('uploading', 'uploading_season') else state_map.get(s.state, str(s.state)),
            'seeds': s.num_seeds,
            'peers': s.num_peers,
            'eta': eta,
            'content_id': t.get('content_id'),
            'upload_progress': t.get('upload_progress', 0),
            'paused': s.paused,
            'season_pack': t.get('season_pack', False),
            'upload_total_files': t.get('upload_total_files', 0),
            'upload_current_file': t.get('upload_current_file', 0),
        })
    return jsonify(result), 200


@app.route('/torrents/update/<info_hash>', methods=['POST'])
@require_auth
def update_torrent(info_hash):
    '''Update metadata on an existing torrent without restarting it.'''
    t = active_torrents.get(info_hash)
    if not t:
        return jsonify({'error': 'Not found'}), 404
    data = request.json or {}
    if 'r2_key' in data:
        t['r2_key'] = data['r2_key']
    if 'content_id' in data:
        t['content_id'] = data['content_id']
    if 'callback_url' in data:
        t['callback_url'] = data['callback_url']
    if 'season_pack' in data:
        t['season_pack'] = data['season_pack']
    log.info(f'[UPDATE] Updated torrent {info_hash}: content_id={t.get("content_id")}, has_r2_key={bool(t.get("r2_key"))}, season_pack={t.get("season_pack")}, status={t["status"]}')

    # If torrent already finished downloading, trigger upload now that we have r2_key
    if t['status'] == 'completed' and t.get('r2_key'):
        handle = t['handle']
        save_path = handle.save_path()
        torrent_info = handle.torrent_file()
        if t.get('season_pack'):
            t['status'] = 'uploading_season'
            threading.Thread(target=_handle_season_pack, args=(info_hash, t, save_path, torrent_info, handle.status().name), daemon=True).start()
        else:
            t['status'] = 'uploading'
            threading.Thread(target=_handle_single_file, args=(info_hash, t, save_path, torrent_info), daemon=True).start()
        log.info(f'[UPDATE] Torrent already complete, starting upload: {info_hash}')

    return jsonify({'status': 'ok'}), 200


@app.route('/torrents/pause/<info_hash>', methods=['POST'])
@require_auth
def pause_torrent(info_hash):
    t = active_torrents.get(info_hash)
    if not t:
        return jsonify({'error': 'Not found'}), 404
    t['handle'].pause()
    return jsonify({'status': 'ok'}), 200


@app.route('/torrents/resume/<info_hash>', methods=['POST'])
@require_auth
def resume_torrent(info_hash):
    t = active_torrents.get(info_hash)
    if not t:
        return jsonify({'error': 'Not found'}), 404
    t['handle'].resume()
    return jsonify({'status': 'ok'}), 200


@app.route('/torrents/delete/<info_hash>', methods=['DELETE'])
@require_auth
def delete_torrent(info_hash):
    t = active_torrents.get(info_hash)
    if not t:
        return jsonify({'error': 'Not found'}), 404
    ses.remove_torrent(t['handle'], lt.options_t.delete_files)
    del active_torrents[info_hash]
    return jsonify({'status': 'ok'}), 200

# -------------------------------------------------------------------
# Background: monitor downloads, upload to R2, idle shutdown
# -------------------------------------------------------------------

VIDEO_EXTENSIONS = {'.mkv', '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.m4v', '.mpg', '.mpeg', '.ts'}
MIN_VIDEO_SIZE = 50 * 1024 * 1024  # 50MB — excludes samples


def parse_episode_info(filename, default_season=None):
    '''Extract season and episode number from a filename.'''
    # S01E01, s01e01, S1E1
    m = re.search(r'[Ss](\d{1,2})\s*[Ee](\d{1,2})', filename)
    if m:
        return int(m.group(1)), int(m.group(2))
    # 1x01, 1X01
    m = re.search(r'(\d{1,2})[xX](\d{1,2})', filename)
    if m:
        return int(m.group(1)), int(m.group(2))
    # Bare E01 with a default season
    if default_season:
        m = re.search(r'[Ee](\d{1,2})', filename)
        if m:
            return default_season, int(m.group(1))
    return None, None


def find_video_files(directory, default_season=None):
    '''Find all video files in a directory, with parsed season/episode info.'''
    results = []
    for root, _, files in os.walk(directory):
        for f in files:
            path = os.path.join(root, f)
            ext = os.path.splitext(f)[1].lower()
            if ext not in VIDEO_EXTENSIONS:
                continue
            size = os.path.getsize(path)
            if size < MIN_VIDEO_SIZE:
                continue
            season, episode = parse_episode_info(f, default_season)
            results.append({
                'path': path,
                'filename': f,
                'size': size,
                'season': season,
                'episode': episode,
                'extension': ext.lstrip('.'),
            })
    # Sort by season, then episode
    results.sort(key=lambda x: (x['season'] or 0, x['episode'] or 0))
    return results


def find_largest_file(directory):
    '''Find the largest file in the torrent download (the actual media file).'''
    largest = None
    largest_size = 0
    for root, _, files in os.walk(directory):
        for f in files:
            path = os.path.join(root, f)
            size = os.path.getsize(path)
            if size > largest_size:
                largest_size = size
                largest = path
    return largest


def upload_to_r2(file_path, r2_key, info_hash):
    '''Upload a file to R2 using multipart upload via the backend.'''
    t = active_torrents.get(info_hash)
    if not t:
        return False

    file_size = os.path.getsize(file_path)
    if not t.get('season_pack'):
        t['status'] = 'uploading'
        t['upload_progress'] = 0

    headers = {'X-API-Key': API_KEY, 'Content-Type': 'application/json'}

    try:
        # Step 1: Get multipart upload URLs from backend
        resp = requests.post(f'{BACKEND_URL}/api/torrents/multipart/create', json={
            'r2_key': r2_key,
            'file_size': file_size,
        }, headers=headers, timeout=30)

        if resp.status_code != 200:
            log.error(f'[UPLOAD] Failed to create multipart: {resp.text}')
            t['status'] = 'upload_failed'
            return False

        multipart = resp.json()
        upload_id = multipart['uploadId']
        parts = multipart['parts']
        total_parts = len(parts)
        log.info(f'[UPLOAD] Multipart created: {total_parts} parts for {file_size} bytes')

        # Step 2: Upload each part
        with open(file_path, 'rb') as f:
            for part in parts:
                chunk = f.read(part['size'])
                part_resp = requests.put(part['url'], data=chunk, headers={
                    'Content-Length': str(len(chunk)),
                }, timeout=600)

                if part_resp.status_code not in (200, 201):
                    log.error(f'[UPLOAD] Part {part["partNumber"]} failed: {part_resp.status_code}')
                    t['status'] = 'upload_failed'
                    return False

                progress = round((part['partNumber'] / total_parts) * 100, 1)
                t['upload_progress'] = progress
                log.info(f'[UPLOAD] Part {part["partNumber"]}/{total_parts} done ({progress}%)')

        # Step 3: Complete multipart upload
        complete_resp = requests.post(f'{BACKEND_URL}/api/torrents/multipart/complete', json={
            'r2_key': r2_key,
            'upload_id': upload_id,
        }, headers=headers, timeout=30)

        if complete_resp.status_code != 200:
            log.error(f'[UPLOAD] Failed to complete multipart: {complete_resp.text}')
            t['status'] = 'upload_failed'
            return False

        t['upload_progress'] = 100
        log.info(f'[UPLOAD] Multipart upload complete: {r2_key}')
        return True

    except Exception as e:
        log.error(f'[UPLOAD] Exception: {e}')
        t['status'] = 'upload_failed'
        return False


def notify_callback(callback_url, info_hash, content_id, status):
    '''Notify the backend of status changes.'''
    if not callback_url:
        return
    try:
        requests.post(callback_url, json={
            'info_hash': info_hash,
            'content_id': content_id,
            'status': status,
        }, headers={'X-API-Key': API_KEY}, timeout=10)
    except Exception:
        pass


def _handle_single_file(info_hash, t, save_path, torrent_info):
    '''Handle a completed single-file torrent download.'''
    global last_activity

    if torrent_info and torrent_info.num_files() == 1:
        file_path = os.path.join(save_path, torrent_info.files().file_path(0))
    else:
        file_path = find_largest_file(save_path)

    if not file_path or not os.path.exists(file_path):
        log.error(f'[MONITOR] File not found after download: {info_hash}')
        t['status'] = 'error'
        return

    r2_key = t.get('r2_key')
    notify_callback(t.get('callback_url'), info_hash, t.get('content_id'), 'uploading')

    file_size = os.path.getsize(file_path)
    log.info(f'[MONITOR] Uploading to R2: {file_path} ({file_size} bytes)')
    success = upload_to_r2(file_path, r2_key, info_hash)
    log.info(f'[MONITOR] R2 upload {"success" if success else "FAILED"}: {info_hash}')
    notify_callback(t.get('callback_url'), info_hash, t.get('content_id'), 'uploaded' if success else 'failed')

    if success:
        ses.remove_torrent(t['handle'], lt.options_t.delete_files)
        del active_torrents[info_hash]
        last_activity = time.time()
        log.info(f'[MONITOR] Cleaned up {info_hash}')


def _handle_season_pack(info_hash, t, save_path, torrent_info, torrent_name):
    '''Handle a completed season pack torrent: discover episodes, register, upload each.'''
    global last_activity

    # Scope to the torrent's own subdirectory (not the shared /tmp/torrents)
    if torrent_info and torrent_info.num_files() > 1:
        first_file = torrent_info.files().file_path(0)
        if '/' in first_file:
            torrent_dir = first_file.split('/')[0]
            save_path = os.path.join(save_path, torrent_dir)

    # Try to extract a default season number from the torrent name
    default_season = None
    if torrent_name:
        m = re.search(r'[Ss](\d{1,2})(?!\s*[Ee]\d)', torrent_name) or re.search(r'\bseason\s*(\d{1,2})\b', torrent_name, re.IGNORECASE)
        if m:
            default_season = int(m.group(1))

    video_files = find_video_files(save_path, default_season)
    episode_files = [f for f in video_files if f['season'] is not None and f['episode'] is not None]

    if not episode_files:
        log.error(f'[SEASON] No episode files found in season pack: {info_hash}')
        t['status'] = 'error'
        notify_callback(t.get('callback_url'), info_hash, t.get('content_id'), 'failed')
        return

    # Deduplicate: keep largest file per (season, episode) pair
    seen = {}
    for ef in episode_files:
        key = (ef['season'], ef['episode'])
        if key not in seen or ef['size'] > seen[key]['size']:
            seen[key] = ef
    episode_files = sorted(seen.values(), key=lambda x: (x['season'], x['episode']))

    log.info(f'[SEASON] Found {len(episode_files)} unique episodes in pack')

    # Call backend to register files and get R2 keys
    headers = {'X-API-Key': API_KEY, 'Content-Type': 'application/json'}
    try:
        resp = requests.post(f'{BACKEND_URL}/api/torrents/season-files', json={
            'content_id': t.get('content_id'),
            'info_hash': info_hash,
            'files': [{
                'filename': f['filename'],
                'size': f['size'],
                'season': f['season'],
                'episode': f['episode'],
                'extension': f['extension'],
            } for f in episode_files],
        }, headers=headers, timeout=30)
    except Exception as e:
        log.error(f'[SEASON] Failed to call season-files endpoint: {e}')
        t['status'] = 'error'
        notify_callback(t.get('callback_url'), info_hash, t.get('content_id'), 'failed')
        return

    if resp.status_code != 200:
        log.error(f'[SEASON] Backend rejected season files: {resp.text}')
        t['status'] = 'error'
        notify_callback(t.get('callback_url'), info_hash, t.get('content_id'), 'failed')
        return

    file_keys = resp.json().get('file_keys', {})

    t['status'] = 'uploading_season'
    t['upload_total_files'] = len(episode_files)
    t['upload_current_file'] = 0
    notify_callback(t.get('callback_url'), info_hash, t.get('content_id'), 'uploading')

    success_count = 0
    for i, ef in enumerate(episode_files):
        r2_key = file_keys.get(ef['filename'])
        if not r2_key:
            log.warning(f'[SEASON] No R2 key for {ef["filename"]}, skipping')
            continue

        t['upload_current_file'] = i + 1
        t['upload_progress'] = round((i / len(episode_files)) * 100, 1)

        log.info(f'[SEASON] Uploading {i+1}/{len(episode_files)}: {ef["filename"]} -> {r2_key}')
        if upload_to_r2(ef['path'], r2_key, info_hash):
            success_count += 1
            last_activity = time.time()  # Reset idle timer after each file
        else:
            log.error(f'[SEASON] Failed to upload {ef["filename"]}')

    t['upload_progress'] = 100

    if success_count > 0:
        notify_callback(t.get('callback_url'), info_hash, t.get('content_id'), 'uploaded')
        ses.remove_torrent(t['handle'], lt.options_t.delete_files)
        del active_torrents[info_hash]
        last_activity = time.time()
        log.info(f'[SEASON] Season pack complete: {success_count}/{len(episode_files)} files uploaded')
    else:
        t['status'] = 'error'
        notify_callback(t.get('callback_url'), info_hash, t.get('content_id'), 'failed')


def monitor_loop():
    '''Background thread: watch for completed downloads, upload to R2, idle shutdown.'''
    global last_activity

    while True:
        time.sleep(2)

        for info_hash, t in list(active_torrents.items()):
            if t['status'] != 'downloading':
                continue

            handle = t['handle']
            s = handle.status()

            # Auto-retry if metadata is stuck (state 2 = downloading_metadata)
            if s.state == 2 and not s.name:
                elapsed = time.time() - t.get('added_at', time.time())
                if elapsed > 60:
                    log.info(f'[MONITOR] Metadata stuck for {int(elapsed)}s, re-adding: {info_hash}')
                    magnet = lt.make_magnet_uri(handle)
                    ses.remove_torrent(handle)
                    params = lt.parse_magnet_uri(magnet)
                    params.save_path = DOWNLOAD_PATH
                    new_handle = ses.add_torrent(params)
                    t['handle'] = new_handle
                    t['added_at'] = time.time()
                    continue

            # Check if download is complete
            if s.progress >= 1.0 and s.state in (4, 5):  # finished or seeding
                log.info(f'[MONITOR] Download complete: {s.name} ({info_hash})')
                handle.pause()  # stop seeding

                save_path = handle.save_path()
                torrent_info = handle.torrent_file()

                if t.get('season_pack'):
                    t['status'] = 'uploading_season'
                    threading.Thread(target=_handle_season_pack, args=(info_hash, t, save_path, torrent_info, s.name), daemon=True).start()
                elif t.get('r2_key'):
                    t['status'] = 'uploading'
                    threading.Thread(target=_handle_single_file, args=(info_hash, t, save_path, torrent_info), daemon=True).start()
                else:
                    # No R2 URL — just mark as done (local download mode)
                    t['status'] = 'completed'
                    last_activity = time.time()
                    log.info(f'[MONITOR] Local download complete: {info_hash}')

        # Idle self-destruct — delete this droplet via DO API
        # Don't shut down if any torrents are uploading or downloading
        has_active_work = any(t['status'] in ('downloading', 'uploading', 'uploading_season') for t in active_torrents.values())
        if IDLE_SHUTDOWN_MINUTES > 0 and not active_torrents and not has_active_work:
            idle_seconds = time.time() - last_activity
            if idle_seconds > IDLE_SHUTDOWN_MINUTES * 60:
                log.info('[MONITOR] Idle timeout reached, self-destructing droplet...')
                try:
                    do_token = os.getenv('DO_API_TOKEN', '')
                    if do_token:
                        # Find our droplet by tag since metadata service is blocked by VPN
                        resp = requests.get(
                            'https://api.digitalocean.com/v2/droplets?tag_name=aperture-torrent',
                            headers={'Authorization': f'Bearer {do_token}'},
                            timeout=10,
                        )
                        droplets = resp.json().get('droplets', [])
                        for d in droplets:
                            droplet_id = d['id']
                            del_resp = requests.delete(
                                f'https://api.digitalocean.com/v2/droplets/{droplet_id}',
                                headers={'Authorization': f'Bearer {do_token}'},
                                timeout=10,
                            )
                            log.info(f'[MONITOR] Self-destruct: droplet {droplet_id}, status {del_resp.status_code}')
                    else:
                        log.error('[MONITOR] Missing DO_API_TOKEN')
                except Exception as e:
                    log.error(f'[MONITOR] Self-destruct failed: {e}')
                os._exit(0)


# Start background monitor
monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
monitor_thread.start()

if __name__ == '__main__':
    port = int(os.getenv('PORT', '8080'))
    app.run(host='0.0.0.0', port=port)
