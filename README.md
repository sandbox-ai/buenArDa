# buenArDa: Un buen Argentinian Dataset destilado de CommonCrawl
## Descripción general

buenArDa es un proyecto destinado a crear un conjunto de datos argentinos de alta calidad destilado del archivo web de CommonCrawl. Este conjunto de datos puede ser utilizado para diversas tareas de procesamiento de lenguaje natural (NLP), incluyendo modelado de lenguaje, clasificación de texto y más.

## Características

- Extrae datos de los índices de CommonCrawl
- Descarga y procesa rangos de bytes específicos de los buckets de S3
- Descomprime y guarda el contenido localmente
- Soporta procesamiento incremental para evitar datos duplicados

## Instalación

Para comenzar con buenArDa, cloná el repositorio e instalá las dependencias requeridas:

```sh
git clone https://github.com/yourusername/buenArDa.git
cd buenArDa
pip install -r requirements.txt
```

## Uso

Para replicar buenArDa, podés correrlo localmente o desplegarlo en un clúster de Kubernetes.

### Ejecución Local

Para crear buenArDa localmente, usá el script `scripts/buenArDa.py`:

```sh
python3 -m scripts.buenArDa --output 
```

Este script se encargará de iniciar el proceso de extracción, descarga y procesamiento de los datos.

### Despliegue en Kubernetes

Si preferís desplegar buenArDa en un clúster de Kubernetes, utiliusáza el script `deploy.sh`:

```sh
./deploy.sh
```

Este script creará los recursos necesarios en tu clúster de Kubernetes y ejecutará los procesos de buenArDa en contenedores.

Asegurate de tener configurado tu entorno de Kubernetes y de tener los permisos necesarios para crear y gestionar recursos en el clúster.

## Licencia

Este proyecto está licenciado bajo la Licencia Apache 2.0. Consulta el archivo [LICENSE](LICENSE) para más detalles.

## Contribuciones

Son bienvenidas las contribuciones! Si tenés alguna sugerencia, abrí un issue o creá un pull request.