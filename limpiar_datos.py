import pandas as pd
from thefuzz import process
import sqlite3


df = pd.read_csv("produccion_unificada.csv", encoding="utf-8")

# Eliminar filas con primera columna vacía
primera_columna = df.columns[0]
df[primera_columna] = df[primera_columna].fillna('')
df[primera_columna] = df[primera_columna].str.replace('\ufeff', '', regex=False).str.strip()
df_limpio = df[df[primera_columna] != ''].copy()

# Convertir a minúsculas
columnas_a_min = ['METAL', 'ETAPA', 'PROCESO', 'CLASIFICACION',
                  'TITULAR', 'UNIDAD', 'REGION', 'PROVINCIA', 'DISTRITO']

for col in columnas_a_min:
    if col in df_limpio.columns:
        df_limpio[col] = df_limpio[col].astype(str).str.lower()


# Quitar tildes
reemplazos = {
    "á": "a", "é": "e", "í": "i", "ó": "o", "ú": "u",
    "ñ": "n", "ü": "u"
}
df_limpio = df_limpio.applymap(
    lambda x: ''.join(reemplazos.get(c, c) for c in str(x)) if pd.notna(x) else x
)

# Reemplazo en CLASIFICACION
if "CLASIFICACION" in df_limpio.columns:
    df_limpio["CLASIFICACION"] = df_limpio["CLASIFICACION"].replace(
        "gran y mediana mineria", "regimen general"
    )

# --- Limpiar TITULAR directamente ---
if "TITULAR" in df_limpio.columns:
    palabras_a_eliminar = [
        "sac", "saa", "eirl", "ltda", "sa", "inc", "corp", "cooperativa",
        "sociedad anonima", "compania", "company", "cooperacion"
    ]
    
    df_limpio["TITULAR"] = (
        df_limpio["TITULAR"]
        .str.replace(r'\.', '', regex=True)  # quitar puntos
        .str.replace(r'\b(?:' + '|'.join(palabras_a_eliminar) + r')\b', '', regex=True)  # quitar palabras exactas
        .str.replace(r'\s+', ' ', regex=True)  # quitar espacios múltiples
        .str.strip()
    )

    # --- Unificación con thefuzz ---
    empresas_unicas = df_limpio["TITULAR"].unique()
    mapeo_empresas = {}

    for empresa in empresas_unicas:
        coincidencias = process.extract(empresa, empresas_unicas, limit=None)
        nombre_estandar = coincidencias[0][0]
        for nombre, score in coincidencias:
            if score >= 90:
                mapeo_empresas[nombre] = nombre_estandar

    df_limpio["TITULAR"] = df_limpio["TITULAR"].map(mapeo_empresas)

# Guardar resultado
df_limpio.to_csv("datos_limpios.csv", index=False, encoding="utf-8")

print(f"Archivo limpio guardado como 'datos_limpios.csv', nombres de empresas unificados.")

#  CONFIGURACIÓN 
csv_file = "datos_limpios.csv"   # CSV limpio que ya preparaste
db_file = "produccion.db"        # Nombre de la base de datos
tabla = "produccion"             # Nombre de la tabla

# LEER CSV 
df = pd.read_csv(csv_file, encoding="utf-8")

# CONECTAR A SQLITE 
conn = sqlite3.connect(db_file)
cursor = conn.cursor()

# CREAR TABLA 
cursor.execute(f"""
CREATE TABLE IF NOT EXISTS {tabla} (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    METAL TEXT,
    ETAPA TEXT,
    PROCESO TEXT,
    CLASIFICACION TEXT,
    TITULAR TEXT,
    UNIDAD TEXT,
    REGION TEXT,
    PROVINCIA TEXT,
    DISTRITO TEXT
)
""")


df.to_sql(tabla, conn, if_exists="replace", index=False)

#  CONFIRMAR Y CERRAR 
conn.commit()
conn.close()

print(f"✅ Base de datos '{db_file}' creada y tabla '{tabla}' importada con éxito.")

