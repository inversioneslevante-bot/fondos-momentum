#!/bin/bash
# ─────────────────────────────────────────────────────────────
# Global Fund Tracker — Arranque completo + URL pública
# Ejecutar: bash start_demo.sh
# ─────────────────────────────────────────────────────────────

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT=5050

echo ""
echo "  📊  Global Fund Tracker"
echo "  ──────────────────────────────────────"

# 1. Matar procesos previos
pkill -f "python3 app.py" 2>/dev/null
pkill -f cloudflared 2>/dev/null
pkill -f "serveo.net" 2>/dev/null
sleep 1

# 2. Arrancar Flask
cd "$APP_DIR"
python3 app.py > /tmp/fondos_app.log 2>&1 &
APP_PID=$!
echo "  ✅  Servidor Flask arrancado (PID $APP_PID)"
sleep 3

# 3. Verificar servidor local
if ! curl -s --max-time 5 "http://localhost:$PORT/api/import-status" > /dev/null; then
  echo "  ❌  Error al arrancar el servidor. Ver /tmp/fondos_app.log"
  exit 1
fi

# 4. Lanzar túnel Cloudflare (URL pública HTTPS)
CLOUDFLARED="$APP_DIR/.cloudflared"
if [ ! -f "$CLOUDFLARED" ]; then
  echo "  ⬇️   Descargando cloudflared..."
  curl -L -s "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-darwin-amd64.tgz" \
    | tar -xz -C "$APP_DIR" && mv "$APP_DIR/cloudflared" "$CLOUDFLARED"
fi

"$CLOUDFLARED" tunnel --url "http://localhost:$PORT" 2>&1 | while IFS= read -r line; do
  if echo "$line" | grep -q "trycloudflare.com"; then
    URL=$(echo "$line" | grep -oP 'https://[^\s]+\.trycloudflare\.com')
    echo ""
    echo "  ══════════════════════════════════════════════════════"
    echo "  🌐  URL PÚBLICA (comparte con tu cliente):"
    echo ""
    echo "      $URL"
    echo ""
    echo "  ══════════════════════════════════════════════════════"
    echo "  ℹ️   La URL cambia cada vez que reinicias el script."
    echo "  ℹ️   Para una URL permanente, despliega en Render.com"
    echo "  ══════════════════════════════════════════════════════"
    echo ""
    echo "  Pulsa CTRL+C para parar todo."
    echo ""
  fi
done &

wait
