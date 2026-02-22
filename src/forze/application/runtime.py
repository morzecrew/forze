from forze.application.kernel.ports import AppRuntimePort
from forze.base.primitives import RuntimeVar

# ----------------------- #

__app_rt: RuntimeVar[AppRuntimePort] = RuntimeVar("app_runtime")


def set_app_runtime(runtime: AppRuntimePort) -> None:
    __app_rt.set_once(runtime)


def get_app_runtime() -> AppRuntimePort:
    return __app_rt.get()
