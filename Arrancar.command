#!/bin/bash
# ─────────────────────────────────────────────────────────────
#  Global Fund Tracker — Doble clic para arrancar
# ─────────────────────────────────────────────────────────────

APP_DIR="$(cd "$(dirname "$0")" && pwd)"
PORT=5050
LOG="/tmp/fondos_app.log"

clear
echo ""
echo "  ╔══════════════════════════════════════╗"
echo "  ║      Global Fund Tracker             ║"
echo "  ╚══════════════════════════════════════╝"
echo ""

# ── 1. Matar procesos previos ──────────────────────────────
pkill -f "python3 app.py" 2>/dev/null
pkill -f cloudflared      2>/dev/null
sleep 1

# ── 2. Comprobar Python ────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  echo "  ❌  Python3 no encontrado. Instálalo desde python.org"
  read -n1 -r -p "  Pulsa cualquier tecla para salir..." _
  exit 1
fi

# ── 3. Comprobar dependencias ─────────────────────────────
cd "$APP_DIR"
MISSING=$(python3 -c "import flask, curl_cffi" 2>&1)
if [ -n "$MISSING" ]; then
  echo "  ⚙️   Instalando dependencias (solo la primera vez)..."
  pip3 install -q -r requirements.txt
fi

# ── 4. Arrancar Flask ─────────────────────────────────────
python3 app.py > "$LOG" 2>&1 &
APP_PID=$!
echo "  ⏳  Arrancando servidor..."

for i in {1..10}; do
  sleep 1
  if curl -s --max-time 2 "http://localhost:$PORT/api/import-status" >/dev/null 2>&1; then
    break
  fi
  if ! kill -0 "$APP_PID" 2>/dev/null; then
    echo "  ❌  Error al arrancar. Revisa el log: $LOG"
    cat "$LOG"
    read -n1 -r -p "  Pulsa cualquier tecla para salir..." _
    exit 1
  fi
done

echo "  ✅  Servidor activo en http://localhost:$PORT"
echo ""

# ── 5. Abrir navegador ────────────────────────────────────
open "http://localhost:$PORT"

# ── 6. Túnel Cloudflare (URL pública opcional) ────────────
CLOUDFLARED="$APP_DIR/.cloudflared"
if [ ! -f "$CLOUDFLARED" ]; then
  echo "  ⬇️   Descargando cloudflared (URL pública)..."
  curl -L -s "https://github.com/cloudflare/cloudflared/releases/latest/download/cloudflared-darwin-amd64.tgz" \
    | tar -xz -C "$APP_DIR" && mv "$APP_DIR/cloudflared" "$CLOUDFLARED"
fi

echo "  🌐  Generando URL pública (espera unos segundos)..."
echo ""

"$CLOUDFLARED" tunnel --url "http://localhost:$PORT" 2>&1 | while IFS= read -r line; do
  if echo "$line" | grep -q "trycloudflare.com"; then
    URL=$(echo "$line" | grep -oE 'https://[^ ]+\.trycloudflare\.com')
    echo "  ╔══════════════════════════════════════════════════════╗"
    echo "  ║  URL PÚBLICA:"
    echo "  ║"
    echo "  ║    $URL"
    echo "  ║"
    echo "  ║  Comparte este enlace con tu cliente."
    echo "  ║  Cambia cada vez que reinicias el programa."
    echo "  ╚══════════════════════════════════════════════════════╝"
    echo ""
    echo "  Pulsa CTRL+C para parar todo."
    echo ""
  fi
done

# ── Limpieza al salir (CTRL+C) ────────────────────────────
trap "kill $APP_PID 2>/dev/null; pkill -f cloudflared 2>/dev/null; echo ''; echo '  🛑  Servidor parado.'" EXIT
wait $APP_PID
