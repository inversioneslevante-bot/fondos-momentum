# Global Fund Tracker (fondos-momentum)

Web app de rankings y simulaciones de momentum sobre ~470 fondos UCITS.

**Web pública:** https://fondos-momentum.onrender.com

## Cómo se mantiene actualizada

Los datos siempre llegan hasta el **último mes completo** (en septiembre → hasta el 31 de agosto).
El mes en curso nunca se guarda.

1. La GitHub Action [`update-data.yml`](.github/workflows/update-data.yml) se ejecuta los días **3 y 6 de cada mes**:
   descarga los NAV mensuales de Morningstar y los benchmarks (S&P 500, Euro Stoxx 50),
   recalcula rentabilidades y sube `data/cache.db` al repositorio.
2. Render detecta el commit en `main` y redespliega la web con los datos nuevos.

No hace falta hacer nada. Si una ejecución falla, GitHub envía un email.
Para lanzarla a mano: pestaña **Actions → Actualizar datos mensuales → Run workflow**.

## Archivos

| Archivo | Qué hace |
|---|---|
| `app.py` | Servidor Flask (rutas web y API) |
| `data_service.py` | Consultas de rankings y estado de datos |
| `backtest.py` | Simulación de las estrategias A–E |
| `fetch_monthly_nav.py` | Descarga NAV mensual de Morningstar (Playwright + Chrome) |
| `fetch_benchmark.py` | Descarga benchmarks vía yfinance |
| `sync_from_nav.py` | Recalcula rentabilidades 1m/3m/6m/YTD/1a y anuales |
| `import_csv.py` / `rentabilidades.csv` | Importación inicial del universo de fondos |
| `analysis_strategies.py` | Script de investigación de estrategias (no lo usa la web) |
| `data/cache.db` | Base de datos SQLite con todos los datos |
| `render.yaml` | Configuración de despliegue en Render |
| `Arrancar.command` / `start_demo.sh` | Arrancar en local (Mac) con URL pública temporal |

## Ejecutar en local

```bash
pip3 install -r requirements.txt curl_cffi playwright
python3 app.py          # http://localhost:5050
```

Actualizar datos a mano (necesita Google Chrome instalado):

```bash
python3 fetch_monthly_nav.py && python3 fetch_benchmark.py && python3 sync_from_nav.py
```
