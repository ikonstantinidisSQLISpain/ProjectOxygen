from __future__ import annotations

import ast
from pathlib import Path


class PythonFlattener:
    """
    Convierte un árbol de módulos Python locales en un único archivo.

    Los imports se consideran absolutos respecto a source_root.

    Ejemplo:

        project/
        ├── main.py
        └── common/
            ├── constants.py
            └── utils.py
    """

    def __init__(self, source_root: str | Path):
        self.source_root = Path(source_root).resolve()

        # Archivos que ya han sido descubiertos.
        self.modules: dict[Path, str] = {}

    def transform_file(
        self,
        file_path: str | Path,
        write_path: str | Path,
    ) -> str:
        """
        Transforma file_path y escribe el resultado en write_path.

        Parameters
        ----------
        file_path:
            Archivo Python principal.

        write_path:
            Ruta donde se escribirá el archivo resultante.

        Returns
        -------
        str
            Contenido generado.
        """

        file_path = Path(file_path).resolve()
        write_path = Path(write_path).resolve()

        if not file_path.exists():
            raise FileNotFoundError(file_path)

        if not file_path.is_file():
            raise ValueError(f"No es un archivo: {file_path}")

        # Descubrimos recursivamente todos los módulos locales.
        self._discover(file_path)

        result = self._generate(file_path)

        # Crear el directorio de destino si no existe.
        write_path.parent.mkdir(
            parents=True,
            exist_ok=True,
        )

        write_path.write_text(
            result,
            encoding="utf-8",
        )

        return result

    # ------------------------------------------------------------------
    # Discovery
    # ------------------------------------------------------------------

    def _discover(self, file_path: Path) -> None:
        file_path = file_path.resolve()

        if file_path in self.modules:
            return

        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))

        module_name = self._module_name(file_path)

        self.modules[file_path] = module_name

        for node in tree.body:
            if not isinstance(node, (ast.Import, ast.ImportFrom)):
                continue

            for dependency in self._resolve_imports(node):
                self._discover(dependency)

    def _module_name(self, file_path: Path) -> str:
        """
        Convierte:

            /project/common/constants.py

        en:

            common.constants
        """

        relative = file_path.relative_to(self.source_root)

        if relative.name == "__init__.py":
            parts = relative.parent.parts
        else:
            parts = relative.with_suffix("").parts

        return ".".join(parts)

    # ------------------------------------------------------------------
    # Import resolution
    # ------------------------------------------------------------------

    def _resolve_imports(self, node: ast.stmt) -> list[Path]:
        result = []

        if isinstance(node, ast.Import):
            for alias in node.names:
                path = self._resolve_module(alias.name)

                if path:
                    result.append(path)

        elif isinstance(node, ast.ImportFrom):
            if node.level != 0:
                raise ValueError(
                    f"Import relativo no soportado: "
                    f"{ast.unparse(node)}"
                )

            if node.module:
                path = self._resolve_module(node.module)

                if path:
                    result.append(path)
                else:
                    # from common import constants
                    for alias in node.names:
                        path = self._resolve_module(
                            f"{node.module}.{alias.name}"
                        )

                        if path:
                            result.append(path)

        return result

    def _resolve_module(self, module: str) -> Path | None:
        """
        Busca un módulo dentro de source_root.

        common.constants
        ->
        source_root/common/constants.py

        También soporta paquetes:

        common
        ->
        source_root/common/__init__.py
        """

        parts = module.split(".")
        base = self.source_root.joinpath(*parts)

        # module.py
        py_file = base.with_suffix(".py")

        if py_file.is_file():
            return py_file.resolve()

        # module/__init__.py
        init_file = base / "__init__.py"

        if init_file.is_file():
            return init_file.resolve()

        return None

    # ------------------------------------------------------------------
    # Code generation
    # ------------------------------------------------------------------

    def _generate(self, main_file: Path) -> str:
        result = [
            "# ============================================================\n",
            "# GENERATED FILE - DO NOT EDIT\n",
            "# ============================================================\n",
            "\n",
            "import types as _types\n",
            "\n",
        ]

        # Generamos primero las dependencias.
        dependencies = [
            path
            for path in self.modules
            if path != main_file
        ]

        for path in dependencies:
            result.append(
                self._generate_module(path)
            )

        # Finalmente el archivo principal.
        result.append(
            "\n# ============================================================\n"
        )
        result.append(
            f"# MAIN: {main_file}\n"
        )
        result.append(
            "# ============================================================\n\n"
        )

        result.append(
            self._generate_main(main_file)
        )

        return "".join(result)

    def _generate_module(self, file_path: Path) -> str:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))

        module_name = self._module_name(file_path)
        namespace_name = self._namespace_name(module_name)

        body = []

        # Creamos el namespace.
        body.append(
            f"{namespace_name} = _types.ModuleType("
            f"{module_name!r})\n"
        )

        # Imports.
        for node in tree.body:
            if isinstance(node, ast.Import):
                for alias in node.names:
                    dependency = self._resolve_module(alias.name)

                    if dependency:
                        dependency_name = self._module_name(
                            dependency
                        )

                        dependency_namespace = self._namespace_name(
                            dependency_name
                        )

                        if alias.asname:
                            # import common.constants as c
                            body.append(
                                f"{namespace_name}.__dict__"
                                f"[{alias.asname!r}] = "
                                f"{dependency_namespace}\n"
                            )
                        else:
                            self._ensure_package_alias(
                                body,
                                namespace_name,
                                alias.name,
                            )

                    else:
                        # Import externo.
                        body.append(
                            self._indent(
                                ast.unparse(node),
                                namespace_name,
                            )
                        )

            elif isinstance(node, ast.ImportFrom):
                dependency = self._resolve_import_from(node)

                if dependency:
                    dependency_name = self._module_name(
                        dependency
                    )

                    dependency_namespace = self._namespace_name(
                        dependency_name
                    )

                    for alias in node.names:
                        if alias.name == "*":
                            body.append(
                                f"{namespace_name}.__dict__.update("
                                f"{dependency_namespace}.__dict__"
                                f")\n"
                            )
                        else:
                            target_name = (
                                alias.asname or alias.name
                            )

                            body.append(
                                f"{namespace_name}.__dict__"
                                f"[{target_name!r}] = "
                                f"{dependency_namespace}.__dict__"
                                f"[{alias.name!r}]\n"
                            )
                else:
                    # Import externo.
                    body.append(
                        self._indent(
                            ast.unparse(node),
                            namespace_name,
                        )
                    )

        # Código normal del módulo.
        normal_nodes = [
            node
            for node in tree.body
            if not isinstance(node, (ast.Import, ast.ImportFrom))
        ]

        if normal_nodes:
            new_tree = ast.Module(
                body=normal_nodes,
                type_ignores=[],
            )

            code = ast.unparse(new_tree)

            body.append(
                self._indent(code, namespace_name)
            )

        return "\n".join(body) + "\n\n"

    def _generate_main(self, file_path: Path) -> str:
        source = file_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(file_path))

        result = []

        for node in tree.body:
            if isinstance(node, ast.Import):
                local = False

                for alias in node.names:
                    dependency = self._resolve_module(alias.name)

                    if dependency:
                        local = True

                        dependency_name = self._module_name(
                            dependency
                        )

                        namespace = self._namespace_name(
                            dependency_name
                        )

                        if alias.asname:
                            result.append(
                                f"{alias.asname} = {namespace}"
                            )
                        else:
                            self._generate_main_import(
                                result,
                                alias.name,
                                namespace,
                            )

                if not local:
                    result.append(ast.unparse(node))

            elif isinstance(node, ast.ImportFrom):
                dependency = self._resolve_import_from(node)

                if dependency:
                    dependency_name = self._module_name(
                        dependency
                    )

                    namespace = self._namespace_name(
                        dependency_name
                    )

                    for alias in node.names:
                        if alias.name == "*":
                            result.append(
                                f"globals().update("
                                f"{namespace}.__dict__)"
                            )
                        else:
                            target = alias.asname or alias.name

                            result.append(
                                f"{target} = "
                                f"{namespace}.__dict__"
                                f"[{alias.name!r}]"
                            )
                else:
                    result.append(ast.unparse(node))

            else:
                result.append(ast.unparse(node))

        return "\n\n".join(result) + "\n"

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _resolve_import_from(
        self,
        node: ast.ImportFrom,
    ) -> Path | None:

        if node.level != 0:
            raise ValueError(
                f"Import relativo no soportado: "
                f"{ast.unparse(node)}"
            )

        if not node.module:
            return None

        # from common.utils import foo
        path = self._resolve_module(node.module)

        if path:
            return path

        # from common import utils
        for alias in node.names:
            path = self._resolve_module(
                f"{node.module}.{alias.name}"
            )

            if path:
                return path

        return None

    def _namespace_name(self, module_name: str) -> str:
        """
        common.constants
        ->
        _module_common_constants
        """

        return (
            "_module_"
            + module_name.replace(".", "_")
        )

    def _ensure_package_alias(
        self,
        result: list[str],
        namespace_name: str,
        module_name: str,
    ) -> None:

        parts = module_name.split(".")

        if len(parts) == 1:
            target = self._namespace_name(module_name)

            result.append(
                f"{namespace_name}.__dict__"
                f"[{parts[0]!r}] = {target}"
            )
            return

        package = parts[0]

        package_namespace = self._namespace_name(package)

        result.append(
            f"{namespace_name}.__dict__"
            f"[{package!r}] = {package_namespace}"
        )

    @staticmethod
    def _indent(
        code: str,
        namespace_name: str,
    ) -> str:
        """
        Ejecuta el código dentro del namespace del módulo.
        """

        return (
            f"exec(compile({code!r}, "
            f"{namespace_name!r}, 'exec'), "
            f"{namespace_name}.__dict__)"
        )


def transform_file(
    file_path: str | Path,
    source_root: str | Path,
    write_path: str | Path,
) -> str:
    """
    API pública.

    Parameters
    ----------
    file_path:
        Archivo Python principal.

    source_root:
        Directorio desde el que se resuelven los imports absolutos.

    write_path:
        Ruta donde se escribirá el archivo resultante.

    Returns
    -------
    str
        Contenido generado.
    """

    flattener = PythonFlattener(source_root)

    return flattener.transform_file(
        file_path,
        write_path,
    )


if __name__ == "__main__":
    input_file = "src2/orchestration/pipelines/all_tables_bronze_silver.py"
    source_root = "src2/orchestration/pipelines/"
    write_path = "src2/orchestration/pipelines/all_tables_bronze_silver_single_file.py"

    transform_file(
        input_file,
        source_root,
        write_path
    )

    print(f"Archivo generado: {write_path}")