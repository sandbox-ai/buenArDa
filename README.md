# buenArDa: Un buen Argentinian Dataset destilado de CommonCrawl
## Descripción general

buenArDa es un proyecto destinado a crear un conjunto de datos argentinos de alta calidad destilado del archivo web de CommonCrawl. Este conjunto de datos puede ser utilizado para diversas tareas de procesamiento de lenguaje natural (NLP), incluyendo modelado de lenguaje, clasificación de texto y más.

## Características

- Extrae datos de los índices de CommonCrawl
- Filtra contenido con [Trafilatura](https://trafilatura.readthedocs.io/en/latest/)
- Descarga y procesa rangos de bytes específicos de los buckets de S3
- Descomprime y guarda el contenido localmente
- Soporta procesamiento incremental para evitar datos duplicados

## Prerequisitos

### Almacenamiento (sólo para kubernetes)

1. Instalá server NFS en el nodo de control, creando la carpeta de salida:
```sh
sudo apt-get update
sudo apt-get install -y nfs-kernel-server
sudo mkdir -p /mnt/buenarda
sudo chown nobody:nogroup /mnt/buenarda
sudo chmod 777 /mnt/buenarda

echo "/mnt/buenarda *(rw,sync,no_subtree_check,no_root_squash)" | sudo tee -a /etc/exports
sudo exportfs -ra
```
2. Instalá NFS en los workers:
```sh
sudo apt-get install -y nfs-common
```

## Instalación

Para comenzar con buenArDa, cloná el repositorio e instalá las dependencias requeridas:

```sh
git clone https://github.com/sandbox-ai/buenArDa
cd buenArDa
# Crear virtual environment, recomendado
python3 -m venv ./.venv
source .venv/bin/activate

pip install -e .
```

## Uso

Para replicar buenArDa, podés correrlo localmente o desplegarlo en un clúster de Kubernetes.

### Ejecución Local

Para crear buenArDa localmente, usá el script `scripts/buenArDa.py`:

```sh
python3 -m scripts.buenArDa --output data.json
```

Este script se encargará de iniciar el proceso de extracción, descarga y procesamiento de los datos.

El argumento --pattern es opcional y por defecto es "*.ar", para filtrar por localización en Argentina.

### Despliegue en Kubernetes

Si preferís desplegar buenArDa en un clúster de Kubernetes, usá el script `deploy.sh`:

```sh
./deploy.sh -i $(hostname -I | awk '{print $1}') [-p pattern]
```
- -i: IP del servidor NFS (requerido)
- -p: Patrón de búsqueda de URLs (opcional, por defecto: "*.ar")
- -t: Modo test (opcional, crea un solo trabajo para verificar que anda el environment de kubernetes)

Este script creará los recursos necesarios en tu clúster de Kubernetes y ejecutará los procesos de buenArDa en contenedores.

Asegurate de tener configurado tu entorno de Kubernetes y de tener los permisos necesarios para crear y gestionar recursos en el clúster.

## Licencia

Este proyecto está licenciado bajo la Licencia Apache 2.0. Consulta el archivo [LICENSE](LICENSE) para más detalles.

## Contribuciones

Son bienvenidas las contribuciones! Si tenés alguna sugerencia, abrí un issue o creá un pull request.