import os
import shutil
from collections import namedtuple

# ---------------------------------------------------------------------------
# Named tuples matching Databricks return types
# ---------------------------------------------------------------------------
FileInfo      = namedtuple("FileInfo",      ["path", "name", "size", "modificationTime"])
MountInfo     = namedtuple("MountInfo",     ["mountPoint", "source", "encryptionType"])
SecretMetadata = namedtuple("SecretMetadata", ["key"])
SecretScope   = namedtuple("SecretScope",   ["name"])


# ===========================================================================
# dbutils.secrets  – full mock backed by environment variables
# ===========================================================================
class SecretsMock:
    """Maps Databricks secret scopes/keys to environment variables.

    Prioridad de búsqueda:
      1. ``SCOPE_KEY`` (variable con prefijo del scope)
      2. ``KEY`` en mayúsculas
      3. ``key`` tal cual (case-sensitive)
    """

    def get(self, scope: str, key: str) -> str:
        # Prioridad: scoped first, luego clave directa
        val = (
            os.getenv(f"{scope}_{key}".upper())
            or os.getenv(key.upper())
            or os.getenv(key)
        )
        if val is None:
            raise ValueError(
                f"Secret '{key}' (scope='{scope}') no encontrado en el entorno. "
                f"Define la variable '{(scope + '_' + key).upper()}' o '{key.upper()}'."
            )
        return val

    def getBytes(self, scope: str, key: str) -> bytes:
        return self.get(scope, key).encode("utf-8")

    def list(self, scope: str):
        prefix = f"{scope}_".upper()
        return [
            SecretMetadata(key=k.replace(prefix, "", 1))
            for k in os.environ
            if k.upper().startswith(prefix)
        ]

    def listScopes(self):
        scopes = set()
        for k in os.environ:
            if "_" in k:
                scopes.add(k.split("_", 1)[0].lower())
        return [SecretScope(name=s) for s in sorted(scopes)]

    def help(self, method=None):
        print("secrets: get, getBytes, list, listScopes")


# ===========================================================================
# dbutils.widgets  – in-memory mock with env-var fallback
# ===========================================================================
class WidgetsMock:

    def __init__(self):
        self._widgets = {}

    def text(self, name: str, defaultValue: str, label: str = None):
        self._widgets.setdefault(name, os.getenv(name, defaultValue))

    def dropdown(self, name: str, defaultValue: str,
                 choices: list, label: str = None):
        self._widgets.setdefault(name, os.getenv(name, defaultValue))

    def combobox(self, name: str, defaultValue: str,
                 choices: list, label: str = None):
        self._widgets.setdefault(name, os.getenv(name, defaultValue))

    def multiselect(self, name: str, defaultValue: str,
                    choices: list, label: str = None):
        self._widgets.setdefault(name, os.getenv(name, defaultValue))

    def get(self, name: str) -> str:
        if name in self._widgets:
            return self._widgets[name]
        val = os.getenv(name)
        if val is not None:
            return val
        raise ValueError(f"Widget '{name}' no encontrado.")

    def getAll(self) -> dict:
        return dict(self._widgets)

    def getArgument(self, name: str, default: str = None) -> str:
        try:
            return self.get(name)
        except ValueError:
            return default

    def remove(self, name: str):
        self._widgets.pop(name, None)

    def removeAll(self):
        self._widgets.clear()

    def help(self, method=None):
        print("widgets: text, dropdown, combobox, multiselect, get, getAll, "
              "getArgument, remove, removeAll")


# ===========================================================================
# dbutils.fs  – soporte para rutas S3A, /Volumes/, dbfs:/ y locales
# ===========================================================================
class FSMock:
    """
    Emula dbutils.fs con soporte para:

    * ``s3a://`` / ``gs://`` / ``abfss://``  →  Hadoop FileSystem
    * ``/Volumes/catalog/schema/volume/…``   →  directorio local (Unity Catalog)
    * ``dbfs:/…``                             →  directorio local (.dbfs/)
    * Rutas locales                           →  sistema de archivos del SO
    """

    def __init__(self, spark=None):
        self._spark = spark

    # ── Resolución de rutas ───────────────────────────────────────────────────

    def _resolve(self, path: str) -> str:
        """Normaliza rutas /Volumes/ y dbfs:/ a rutas concretas."""
        from databricks_shim.unity_catalog import (
            is_volume_path, is_dbfs_path,
            resolve_volume_path, resolve_dbfs_path,
        )
        if is_volume_path(path):
            return resolve_volume_path(path)
        if is_dbfs_path(path):
            return resolve_dbfs_path(path)
        return path

    def _is_remote(self, path: str) -> bool:
        return "://" in path

    # ── Hadoop FS helpers ─────────────────────────────────────────────────────

    def _hadoop_fs(self, path: str):
        from py4j.java_gateway import java_import
        jvm = self._spark._jvm
        java_import(jvm, "org.apache.hadoop.fs.Path")
        java_import(jvm, "org.apache.hadoop.fs.FileSystem")
        hpath = jvm.org.apache.hadoop.fs.Path(path)
        conf  = self._spark._jsc.hadoopConfiguration()
        fs    = jvm.org.apache.hadoop.fs.FileSystem.get(hpath.toUri(), conf)
        return fs, hpath

    # ── API pública ───────────────────────────────────────────────────────────

    def ls(self, path: str):
        """Lista archivos/directorios.

        Para rutas /Volumes/... y dbfs:/..., las entradas devueltas preservan
        el prefijo UC original (igual que Databricks).  Ejemplo:
            ls("/Volumes/main/bronze/raw/") →
                FileInfo(path='/Volumes/main/bronze/raw/data.csv', ...)
        """
        original_path = path.rstrip("/")
        resolved = self._resolve(path)

        if self._spark and self._is_remote(resolved):
            fs, hpath = self._hadoop_fs(resolved)
            statuses = fs.listStatus(hpath)
            results = []
            for st in statuses:
                p    = st.getPath().toString()
                name = st.getPath().getName()
                if st.isDirectory():
                    name += "/"
                results.append(FileInfo(
                    path=p, name=name,
                    size=st.getLen(),
                    modificationTime=st.getModificationTime(),
                ))
            return results

        import pathlib as _pl
        from databricks_shim.unity_catalog import (
            is_volume_path, is_dbfs_path,
            _volumes_root, _dbfs_root,
        )
        p = _pl.Path(resolved)
        if not p.exists():
            raise FileNotFoundError(f"Path does not exist: {path}")

        results = []
        for child in sorted(p.iterdir()):
            is_dir = child.is_dir()
            # Reconstruir la ruta en formato UC si el path original era UC
            if is_volume_path(path):
                # /Volumes/cat/sch/vol/... → preservar prefijo /Volumes/
                vr = _volumes_root().rstrip("/")
                rel = str(child).replace(vr, "", 1).lstrip("/")
                child_path = "/Volumes/" + rel
            elif is_dbfs_path(path):
                # dbfs:/... → preservar prefijo dbfs:/
                dr = _dbfs_root().rstrip("/")
                rel = str(child).replace(dr, "", 1).lstrip("/")
                child_path = "dbfs:/" + rel
            else:
                child_path = str(child)
            if is_dir and not child_path.endswith("/"):
                child_path += "/"
            results.append(FileInfo(
                path=child_path,
                name=child.name + ("/" if is_dir else ""),
                size=child.stat().st_size if not is_dir else 0,
                modificationTime=int(child.stat().st_mtime * 1000),
            ))
        return results

    def head(self, path: str, max_bytes: int = 65536) -> str:
        path = self._resolve(path)
        if self._spark and self._is_remote(path):
            fs, hpath = self._hadoop_fs(path)
            stream = fs.open(hpath)
            buf  = bytearray(max_bytes)
            from py4j.java_gateway import java_import
            java_import(self._spark._jvm, "java.io.BufferedInputStream")
            bis    = self._spark._jvm.java.io.BufferedInputStream(stream)
            n_read = bis.read(buf, 0, max_bytes)
            bis.close()
            return bytes(buf[:max(n_read, 0)]).decode("utf-8", errors="replace")
        with open(path, "rb") as f:
            return f.read(max_bytes).decode("utf-8", errors="replace")

    def cp(self, src: str, dst: str, recurse: bool = False) -> bool:
        src = self._resolve(src)
        dst = self._resolve(dst)
        if self._spark and (self._is_remote(src) or self._is_remote(dst)):
            fs_src, hp_src = self._hadoop_fs(src)
            _, hp_dst      = self._hadoop_fs(dst)
            from py4j.java_gateway import java_import
            jvm = self._spark._jvm
            java_import(jvm, "org.apache.hadoop.fs.FileUtil")
            conf   = self._spark._jsc.hadoopConfiguration()
            fs_dst = jvm.org.apache.hadoop.fs.FileSystem.get(hp_dst.toUri(), conf)
            jvm.org.apache.hadoop.fs.FileUtil.copy(
                fs_src, hp_src, fs_dst, hp_dst, False, conf)
            return True
        if os.path.isdir(src) and recurse:
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
            shutil.copy2(src, dst)
        return True

    def mv(self, src: str, dst: str, recurse: bool = False) -> bool:
        self.cp(src, dst, recurse=recurse)
        self.rm(src, recurse=recurse)
        return True

    def rm(self, path: str, recurse: bool = False) -> bool:
        path = self._resolve(path)
        if self._spark and self._is_remote(path):
            fs, hpath = self._hadoop_fs(path)
            fs.delete(hpath, recurse)
            return True
        if os.path.isdir(path):
            if recurse:
                shutil.rmtree(path)
            else:
                os.rmdir(path)
        elif os.path.exists(path):
            os.remove(path)
        return True

    def mkdirs(self, path: str) -> bool:
        path = self._resolve(path)
        if self._spark and self._is_remote(path):
            fs, hpath = self._hadoop_fs(path)
            fs.mkdirs(hpath)
            return True
        os.makedirs(path, exist_ok=True)
        return True

    def put(self, path: str, contents: str, overwrite: bool = False) -> bool:
        path = self._resolve(path)
        if self._spark and self._is_remote(path):
            fs, hpath = self._hadoop_fs(path)
            if not overwrite and fs.exists(hpath):
                raise FileExistsError(f"Path already exists: {path}")
            stream = fs.create(hpath, overwrite)
            data   = contents.encode("utf-8")
            stream.write(bytearray(data))
            stream.close()
            return True
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
        mode = "w" if overwrite else "x"
        with open(path, mode, encoding="utf-8") as f:
            f.write(contents)
        return True

    # ── Mounts (no-op — concepto DBFS/Databricks) ────────────────────────────

    def mount(self, source: str, mountPoint: str,
              encryptionType: str = "", owner=None,
              extra_configs: dict = None) -> bool:
        print(f"[Mock] mount {source} → {mountPoint} (no-op locally)")
        return True

    def updateMount(self, source: str, mountPoint: str,
                    encryptionType: str = "", owner=None,
                    extra_configs: dict = None) -> bool:
        print(f"[Mock] updateMount {source} → {mountPoint} (no-op locally)")
        return True

    def mounts(self):
        return []

    def unmount(self, mountPoint: str) -> bool:
        print(f"[Mock] unmount {mountPoint} (no-op locally)")
        return True

    def refreshMounts(self) -> bool:
        return True

    def help(self, method=None):
        print(
            "fs: ls, head, cp, mv, rm, mkdirs, put\n"
            "    mount, updateMount, mounts, unmount, refreshMounts\n"
            "Rutas soportadas: s3a://, /Volumes/, dbfs:/, rutas locales"
        )


# ===========================================================================
# dbutils.credentials  – AWS credential management (no-op locally)
# ===========================================================================
class CredentialsMock:
    """Mock para dbutils.credentials (específico de AWS). No-op localmente."""

    def assumeRole(self, arn: str) -> bool:
        print(f"[Mock] assumeRole {arn} (no-op locally)")
        return True

    def showCurrentRole(self):
        return []

    def showRoles(self):
        return []

    def getServiceCredentialsProvider(self, credential_name: str):
        """Retorna un proveedor de credenciales de servicio (Unity Catalog).

        En Databricks real devuelve un credential provider para acceder a
        servicios externos (Azure, GCP, etc.).  Localmente retorna un objeto
        de credentials vacío como no-op.

        Args:
            credential_name: Nombre de la service credential registrada en UC.

        Returns:
            Un dict con campos compatibles (access_token, expiry) vacíos.
        """
        import warnings
        warnings.warn(
            f"[DBUtils] getServiceCredentialsProvider('{credential_name}') — "
            "no-op local (Unity Catalog service credentials no disponibles en emulación)",
            stacklevel=2,
        )
        return {"access_token": "", "expiry": None, "credential_name": credential_name}

    def help(self, method=None):
        print("credentials: assumeRole, showCurrentRole, showRoles, "
              "getServiceCredentialsProvider")


# ===========================================================================
# dbutils.notebook  – run / exit / context mock
# ===========================================================================
class NotebookContextMock:
    """Mock para dbutils.notebook.entry_point.getDbutils().notebook().getContext()."""

    def __init__(self):
        self._tags = {
            "orgId":         os.getenv("DATABRICKS_ORG_ID",      "0"),
            "clusterId":     os.getenv("DATABRICKS_CLUSTER_ID",  "local-cluster"),
            "clusterName":   os.getenv("DATABRICKS_CLUSTER_NAME","databricks-local"),
            "notebookPath":  os.getenv("DATABRICKS_NOTEBOOK_PATH", "/local/notebook"),
            "user":          os.getenv("DATABRICKS_USER",
                                        os.getenv("USER", "local-user")),
            "notebookId":    os.getenv("DATABRICKS_NOTEBOOK_ID", "0"),
            "currentCatalog": os.getenv("DATABRICKS_CATALOG",   "main"),
        }

    def toJson(self) -> str:
        import json
        return json.dumps({"tags": self._tags})

    def tags(self) -> dict:
        return self._tags


class EntryPointMock:
    def __init__(self):
        self._context = NotebookContextMock()

    def getContext(self) -> NotebookContextMock:
        return self._context


class GetDbutilsMock:
    def __init__(self):
        self._notebook = EntryPointMock()

    def notebook(self) -> EntryPointMock:
        return self._notebook


class EntryPointRootMock:
    def __init__(self):
        self._getDbutils = GetDbutilsMock()

    def getDbutils(self) -> GetDbutilsMock:
        return self._getDbutils


class NotebookMock:

    def __init__(self):
        self.entry_point = EntryPointRootMock()

    def run(self, path: str, timeout_seconds: int = 0,
            arguments: dict = None) -> str:
        import importlib.util
        print(f"[Mock] Running notebook: {path}")
        abs_path = (path if os.path.isabs(path)
                    else os.path.join(os.getcwd(), path))
        if not abs_path.endswith(".py"):
            abs_path += ".py"
        if not os.path.exists(abs_path):
            raise FileNotFoundError(f"Notebook not found: {abs_path}")
        if arguments:
            for k, v in arguments.items():
                os.environ[k] = str(v)
        spec = importlib.util.spec_from_file_location("_notebook_run", abs_path)
        mod  = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(mod)
        except SystemExit as e:
            return str(e.code) if e.code is not None else ""
        return ""

    def exit(self, value: str = "") -> None:
        print(f"Notebook exited: {value}")
        raise SystemExit(value)

    def help(self, method=None):
        print("notebook: run, exit, entry_point")


# ===========================================================================
# dbutils.jobs.taskValues  – in-memory mock
# ===========================================================================
class TaskValuesMock:

    def __init__(self):
        self._store: dict = {}

    def set(self, key: str, value) -> None:
        self._store[key] = value

    def get(self, taskKey: str, key: str,
            default=None, debugValue=None):
        compound = f"{taskKey}::{key}"
        if compound in self._store:
            return self._store[compound]
        if key in self._store:
            return self._store[key]
        if debugValue is not None:
            return debugValue
        if default is not None:
            return default
        raise ValueError(
            f"Task value '{key}' no encontrado para la tarea '{taskKey}'."
        )

    def help(self, method=None):
        print("taskValues: get, set")


class JobsMock:
    def __init__(self):
        self.taskValues = TaskValuesMock()

    def help(self, method=None):
        print("jobs: taskValues")


# ===========================================================================
# dbutils.data  – summarize mock
# ===========================================================================
class DataMock:
    def summarize(self, df, precise: bool = False) -> None:
        if hasattr(df, "describe"):
            df.describe().show()
        elif hasattr(df, "summary"):
            df.summary().show()
        else:
            print("[Mock] summarize not available for this DataFrame type.")

    def help(self, method=None):
        print("data: summarize")


# ===========================================================================
# DBUtilsShim  – ensambla todos los módulos
# ===========================================================================
class DBUtilsShim:
    """
    Shim de alta fidelidad para Databricks DBUtils en entornos locales.
    Cubre: credentials, data, fs, jobs, notebook, secrets, widgets.
    """

    def __init__(self, spark=None):
        self.credentials = CredentialsMock()
        self.data        = DataMock()
        self.fs          = FSMock(spark=spark)
        self.jobs        = JobsMock()
        self.notebook    = NotebookMock()
        self.secrets     = SecretsMock()
        self.widgets     = WidgetsMock()

    def help(self) -> None:
        print("Available modules: credentials, data, fs, jobs, "
              "notebook, secrets, widgets")


# ===========================================================================
# Fábricas públicas
# ===========================================================================
def get_dbutils(spark) -> DBUtilsShim:
    """Devuelve DBUtils — real en Databricks, shim localmente."""
    try:
        from pyspark.dbutils import DBUtils
        return DBUtils(spark)
    except ImportError:
        return DBUtilsShim(spark)


# ===========================================================================
# display()  – función display compatible con Databricks
# ===========================================================================
def display(df_or_data=None, maxRows: int = 1000,
            maxColumns: int = 20, truncate: bool = True) -> None:
    """
    Emula la función display() de Databricks.

    * maxRows     : máximo de filas a mostrar (default 1000)
    * maxColumns  : máximo de columnas       (default 20)
    * truncate    : truncar strings largos   (default True)
    """
    if df_or_data is None:
        return

    # Spark DataFrame → convertir a Pandas y mostrar en Jupyter
    if hasattr(df_or_data, "toPandas"):
        try:
            from IPython.display import display as ipy_display
            pdf = df_or_data.limit(maxRows).toPandas()
            ipy_display(pdf)
        except ImportError:
            df_or_data.show(maxRows, truncate=truncate)
        return

    # Pandas DataFrame → HTML en Jupyter
    if hasattr(df_or_data, "_repr_html_"):
        try:
            from IPython.display import display as ipy_display, HTML
            n = len(df_or_data)
            truncated = df_or_data.head(maxRows) if n > maxRows else df_or_data
            ipy_display(HTML(truncated._repr_html_()))
        except ImportError:
            print(df_or_data)
        return

    # Fallback genérico
    try:
        from IPython.display import display as ipy_display
        ipy_display(df_or_data)
    except ImportError:
        print(df_or_data)
