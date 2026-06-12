import importlib
import importlib.util
import os
import sys
from pathlib import Path


PLUGINS_REPO = Path(__file__).resolve().parents[1]
MOVIEPILOT_ROOT = Path(os.environ.get("MOVIEPILOT_ROOT", r"C:\tmp\MoviePilot")).resolve()

if str(MOVIEPILOT_ROOT) not in sys.path:
    sys.path.insert(0, str(MOVIEPILOT_ROOT))

from app.testing.bootstrap import ensure_optional_stub, prepare_v1_backend, prepare_v2_backend


def _unload_autosignin_modules() -> None:
    for name in list(sys.modules):
        if name == "app.plugins.autosignin" or name.startswith("app.plugins.autosignin."):
            sys.modules.pop(name, None)


def load_autosignin_module(generation: str):
    generation = str(generation).lower()
    if generation == "v2":
        prepare_v2_backend(PLUGINS_REPO)
        package_dir = PLUGINS_REPO / "plugins.v2" / "autosignin"
    elif generation == "v1":
        prepare_v1_backend(PLUGINS_REPO)
        ensure_optional_stub(
            "app.db.sitestatistic_oper",
            SiteStatisticOper=type("SiteStatisticOper", (), {}),
        )
        package_dir = PLUGINS_REPO / "plugins" / "autosignin"
    else:
        raise ValueError(f"Unsupported generation: {generation}")

    _unload_autosignin_modules()

    spec = importlib.util.spec_from_file_location(
        "app.plugins.autosignin",
        package_dir / "__init__.py",
        submodule_search_locations=[str(package_dir)],
    )
    if spec is None or spec.loader is None:
        raise RuntimeError(f"Unable to load autosignin package from {package_dir}")

    module = importlib.util.module_from_spec(spec)
    sys.modules["app.plugins.autosignin"] = module
    spec.loader.exec_module(module)
    return module


def load_autosignin_captcha_module(generation: str):
    load_autosignin_module(generation)
    return importlib.import_module("app.plugins.autosignin.captcha")
