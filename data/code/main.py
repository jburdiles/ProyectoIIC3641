import argparse
import asyncio
import csv
import sys
from pathlib import Path
from typing import List
from urllib.parse import urlparse
import pandas as pd

from playwright.async_api import async_playwright


# ==============================
#  CONFIGURACIÓN DE ENLACES
# ==============================

def load_match_data(csv_path: str):
    """Carga el CSV con columnas ID_MATCH y NAME."""
    df = pd.read_csv(csv_path)
    data = df.to_dict(orient="records")
    return data

def construct_links(data):
    """Construye los enlaces dinámicamente a partir del CSV."""
    return [
        f"https://www.transfermarkt.es/{row['NAME']}/statistik/spielbericht/{row['ID_MATCH']}"
        for row in data
    ]

# Cargar el CSV con los partidos
data = load_match_data("matches_id_name.csv")
links = construct_links(data)


# ==============================
#  FUNCIONES AUXILIARES
# ==============================

def parse_url_info(url: str):
    """Extrae Equipo_1, Equipo_2 e ID de la URL."""
    path = urlparse(url).path.strip("/").split("/")
    equipos = path[0].split("_")
    equipo_1 = equipos[0]
    equipo_2 = equipos[1] if len(equipos) > 1 else None
    match_id = path[-1]
    return equipo_1, equipo_2, match_id


def chunked(iterable: List[str], n: int):
    """Divide una lista en bloques de tamaño n."""
    for i in range(0, len(iterable), n):
        yield iterable[i : i + n]


# ==============================
#  FUNCIÓN PRINCIPAL DE SCRAPING
# ==============================

async def fetch_page(page, url: str, retries: int = 2, base_wait: float = 1.0):
    """Abre la página y extrae todos los datos necesarios."""
    for attempt in range(retries + 1):
        try:
            await page.goto(url, timeout=30000)

            # --- Esperar a que se carguen las estadísticas ---
            await page.wait_for_selector(".sb-statistik-zahl", timeout=10000)

            # --- Extraer estadísticas ---
            elements = await page.locator(".sb-statistik-zahl").all()
            extracted_data = [await el.inner_text() for el in elements]

            # --- Extraer fecha ---
            fecha_elem = page.locator("a[href*='datum/']")
            fecha_href = None
            fecha_text = None
            if await fecha_elem.count() > 0:
                fecha_href = await fecha_elem.first.get_attribute("href")
                fecha_text = await fecha_elem.first.inner_text()
            fecha = fecha_href.split("/datum/")[-1] if fecha_href else None

            # --- Extraer valores del gráfico (posesión) ---
            posesiones = []
            try:
                await page.wait_for_selector("svg tspan", timeout=10000)
                tspans = await page.locator("svg tspan").all()
                for t in tspans:
                    text = (await t.text_content() or "").strip()
                    if text.isdigit():
                        posesiones.append(text)
            except Exception as e:
                print(f"[WARN] No se encontró valor de gráfico en {url}: {e}")
                posesiones = []

            posesion_1 = posesiones[1] if len(posesiones) > 1 else None
            posesion_2 = posesiones[0] if len(posesiones) > 0 else None

            # --- Extraer resultado final (goles) ---
            goles_1 = None
            goles_2 = None
            try:
                await page.wait_for_selector(".sb-endstand", timeout=5000)
                marcador = await page.locator(".sb-endstand").first.text_content()
                if marcador:
                    marcador = marcador.strip().replace(" ", "")
                    if ":" in marcador:
                        partes = marcador.split(":")
                        goles_1 = partes[0]
                        goles_2 = partes[1][0]
            except Exception as e:
                print(f"[WARN] No se encontró marcador en {url}: {e}")

            # --- Extraer equipos e ID ---
            equipo_1, equipo_2, match_id = parse_url_info(url)

            return {
                "url": url,
                "equipo_1": equipo_1,
                "equipo_2": equipo_2,
                "id_match": match_id,
                "fecha": fecha,
                "fecha_texto": fecha_text.strip() if fecha_text else None,
                "posesion_1": posesion_1,
                "posesion_2": posesion_2,
                "goles_1": goles_1,
                "goles_2": goles_2,
                "data": extracted_data
            }

        except Exception as e:
            last_err = e
            if attempt < retries:
                await asyncio.sleep(base_wait * (2 ** attempt))
            else:
                return {"url": url, "error": repr(last_err)}


# ==============================
#  LÓGICA PRINCIPAL
# ==============================

async def run(
    links: List[str],
    batch_size: int,
    concurrency: int,
    output_path: Path,
    headless: bool,
    retries: int,
    base_wait: float
):
    """Ejecuta scraping en lotes y guarda resultados en CSV."""
    sem = asyncio.Semaphore(concurrency)
    write_lock = asyncio.Lock()

    async with async_playwright() as pw:
        browser = await pw.chromium.launch(headless=headless)

        # --- Crear contexto con cookie de consentimiento ---
        context = await browser.new_context()

        # Inyectar cookie de consentimiento (simula que ya aceptaste las cookies)
        await context.add_cookies([
            {
                "name": "tm_consent",
                "value": "true",
                "domain": "www.transfermarkt.es",
                "path": "/"
            },
            {
                "name": "tm_consent",
                "value": "true",
                "domain": "transfermarkt.es",
                "path": "/"
            }
        ])

        csv_path = output_path.with_suffix('.csv')
        with csv_path.open("w", encoding="utf-8", newline="") as csvfile:
            csv_writer = csv.writer(csvfile)

            # --- Escribir encabezado ---
            csv_writer.writerow([
                "url", "equipo_1", "equipo_2", "match_id",
                "fecha", "fecha_texto", "posesion_1", "posesion_2",
                "goles_1", "goles_2",
                "disparos_totales_1", "disparos_totales_2",
                "disparos_fuera_1", "disparos_fuera_2",
                "paraden_1", "paraden_2",
                "corners_1", "corners_2",
                "tiros_libres_1", "tiros_libres_2",
                "faltas_1", "faltas_2",
                "fueras_de_juego_1", "fueras_de_juego_2"
            ])

            async def process_batch(urls: List[str]):
                async def worker(url: str):
                    async with sem:
                        page = await context.new_page()
                        try:
                            res = await fetch_page(page, url, retries=retries, base_wait=base_wait)
                        finally:
                            await page.close()

                        async with write_lock:
                            if "data" in res:
                                row = [
                                    res["url"],
                                    res.get("equipo_1"),
                                    res.get("equipo_2"),
                                    res.get("id_match"),
                                    res.get("fecha"),
                                    res.get("fecha_texto"),
                                    res.get("posesion_1"),
                                    res.get("posesion_2"),
                                    res.get("goles_1"),
                                    res.get("goles_2")
                                ]
                                # Añadir las 14 estadísticas
                                row.extend(res["data"][:14])
                                csv_writer.writerow(row)
                            else:
                                csv_writer.writerow([res["url"], "error"])

                tasks = [asyncio.create_task(worker(url)) for url in urls]
                await asyncio.gather(*tasks)

            # --- Procesar en lotes ---
            for batch_i, batch in enumerate(chunked(links, batch_size), start=1):
                print(f"Processing batch {batch_i}: {len(batch)} urls (concurrency={concurrency})")
                await process_batch(batch)

        await browser.close()


# ==============================
#  PARÁMETROS Y MAIN
# ==============================

def parse_args():
    p = argparse.ArgumentParser(description="Playwright scraper completo Transfermarkt (sin banner de cookies)")
    p.add_argument("--batch-size", type=int, default=100, help="URLs por lote")
    p.add_argument("--concurrency", type=int, default=50, help="Máximo de páginas simultáneas")
    p.add_argument("--output", type=Path, default=Path("output.csv"), help="Archivo CSV de salida")
    p.add_argument("--headless", action="store_true", help="Ejecutar navegador en modo headless (sin ventana)")
    p.add_argument("--retries", type=int, default=2, help="Reintentos por URL")
    p.add_argument("--base-wait", type=float, default=1.0, help="Espera base para reintentos")
    return p.parse_args()


def main():
    args = parse_args()

    if not links:
        print("No hay enlaces para procesar.")
        sys.exit(0)

    print(f"🕷️ Iniciando scraping de {len(links)} URLs")
    print(f"📄 Guardando en: {args.output}")

    asyncio.run(run(
        links=links,
        batch_size=args.batch_size,
        concurrency=args.concurrency,
        output_path=args.output,
        headless=args.headless,
        retries=args.retries,
        base_wait=args.base_wait
    ))

    print("✅ Scraping completado.")


if __name__ == "__main__":
    main()
