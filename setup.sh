#!/bin/bash
# Server setup script for Digital Ocean droplet
# Run as root after first boot

set -e

echo "=== Updating system ==="
apt-get update && apt-get upgrade -y

echo "=== Installing dependencies ==="
apt-get install -y python3 python3-pip python3-venv python3-libtorrent nginx

echo "=== Setting up app directory ==="
mkdir -p /var/www/html
cd /var/www/html

echo "=== Creating virtual environment ==="
python3 -m venv venv
source venv/bin/activate
pip install flask flask_cors requests gunicorn

echo "=== Creating download directory ==="
mkdir -p /tmp/torrents

echo "=== Setting up systemd service ==="
cat > /etc/systemd/system/torrent-service.service << 'EOF'
[Unit]
Description=Hash Torrent Service
After=network.target

[Service]
Type=simple
User=root
WorkingDirectory=/var/www/html
EnvironmentFile=/var/www/html/.env
ExecStart=/var/www/html/venv/bin/gunicorn --bind 0.0.0.0:8080 --workers 1 --threads 4 --timeout 120 app:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF

echo "=== Enabling service ==="
systemctl daemon-reload
systemctl enable torrent-service

echo "=== Installing NordVPN ==="
sh <(curl -sSf https://downloads.nordcdn.com/apps/linux/install.sh)

echo ""
echo "=== Setup complete ==="
echo "Next steps:"
echo "  1. Copy .env file: cp .env.example .env && nano .env"
echo "  2. Login to NordVPN: nordvpn login"
echo "  3. Enable NordLynx: nordvpn set technology nordlynx"
echo "  4. Auto-connect: nordvpn set autoconnect on"
echo "  5. Connect: nordvpn connect"
echo "  6. Start service: systemctl start torrent-service"
echo "  7. Test: curl http://localhost:8080/health"
echo "  8. Snapshot this droplet!"
