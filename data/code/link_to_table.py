import pandas as pd

# Cargar el archivo original
df = pd.read_csv("ligas_matches.csv")

# Tomar la segunda columna (índice 1)
rutas = df.iloc[:, 1].astype(str)

id_match = []
name = []

for ruta in rutas:
    try:
        # Limpia espacios y barras
        ruta = ruta.strip().strip('/')
        partes = ruta.split('/')
        # El ID_MATCH es el último segmento
        match_id = partes[-1]
        # El NAME está antes de "index"
        if "index" in partes:
            name_part = partes[partes.index("index") - 1]
        else:
            name_part = partes[-2]
        id_match.append(match_id)
        name.append(name_part)
    except Exception as e:
        print(f"Error procesando '{ruta}': {e}")
        id_match.append(None)
        name.append(None)

# Crear nuevo DataFrame
df_out = pd.DataFrame({
    "ID_MATCH": id_match,
    "NAME": name
})

# Guardar resultado
df_out.to_csv("matches_id_name.csv", index=False, encoding="utf-8")

print("✅ Archivo generado: matches_id_name.csv")
