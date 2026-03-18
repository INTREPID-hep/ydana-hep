import threading
import time
from typing import Any

from dask.callbacks import Callback
from tqdm.auto import tqdm


class DummyPbar:
    """Null Object pattern to safely ignore progress updates.

    Provides a no-op context manager and ``update`` method. This allows
    the main execution code to blindly call ``pbar.update()`` without
    needing to wrap every UI update in an ``if show_progress:`` block.
    """

    def update(self, *args: Any, **kwargs: Any) -> None:
        pass

    def __enter__(self) -> "DummyPbar":
        return self

    def __exit__(self, *args: Any) -> None:
        pass


class CustomTqdmDaskCallback(Callback):
    """Custom Dask callback using asynchronous polling and tqdm.

    Standard Dask callbacks run their ``_posttask`` method on the main thread
    every time a micro-task finishes. For massive task graphs (100k+ nodes),
    this cripples performance. This custom callback bypasses that overhead
    by spawning a lightweight background thread that periodically polls
    Dask's internal state to update a ``tqdm`` progress bar.

    Parameters
    ----------
    poll_interval : float, optional
        Time in seconds the background thread sleeps between checking Dask's
        progress. A smaller value (e.g., 0.1) makes the bar update smoother
        but uses slightly more CPU. A larger value (e.g., 1.0) reduces UI
        overhead on massive clusters but makes the bar look choppy.
        Defaults to 0.2 seconds.
    **tqdm_kwargs : dict
        Any valid keyword argument accepted by ``tqdm.tqdm`` (e.g., ``desc``,
        ``colour``, ``delay``, ``leave``, ``unit``).
    """

    def __init__(self, poll_interval: float = 0.2, **tqdm_kwargs: Any) -> None:
        self.poll_interval = poll_interval
        self.tqdm_kwargs = tqdm_kwargs
        self.pbar = None
        self._running = False
        self._thread = None

    def _start_state(self, dsk: Any, state: dict[str, Any]) -> None:
        # Calculate total tasks from Dask's internal state
        total = sum(
            len(state[k]) for k in ["ready", "waiting", "running", "finished"] if k in state
        )

        # Merge the dynamically calculated total with the user's kwargs
        kwargs = self.tqdm_kwargs.copy()
        kwargs.setdefault("total", total)

        self.pbar = tqdm(**kwargs)
        self._running = True

        # Background worker to poll Dask's state
        def _poll_dask_state() -> None:
            last_finished = 0
            while self._running:
                current_finished = len(state.get("finished", []))
                if current_finished > last_finished:
                    self.pbar.update(current_finished - last_finished)
                    last_finished = current_finished
                time.sleep(self.poll_interval)

        self._thread = threading.Thread(target=_poll_dask_state, daemon=True)
        self._thread.start()

    def _finish(self, dsk: Any, state: dict[str, Any], errored: bool) -> None:
        self._running = False
        if self._thread is not None:
            self._thread.join(timeout=1.0)

        if self.pbar is not None:
            final_finished = len(state.get("finished", []))
            if final_finished > self.pbar.n:
                self.pbar.update(final_finished - self.pbar.n)
            self.pbar.close()


class ProgressBarFactory:
    """A Factory that yields the correct context manager for tracking progress.

    Dynamically routes progress tracking based on the execution mode, allowing
    the orchestrator to use the exact same ``with ProgressProxy(...) as pbar:``
    syntax for both eager loops and lazy Dask computations.

    Parameters
    ----------
    mode : {"eager", "lazy"}
        The execution path. ``"eager"`` returns a standard synchronous ``tqdm``
        bar. ``"lazy"`` returns the asynchronous ``CustomTqdmDaskCallback``.
    show : bool, optional
        If False, returns a ``DummyPbar`` that silently swallows all updates,
        ensuring zero UI overhead in batch cluster environments. Defaults to True.
    **tqdm_kwargs : dict
        Passed directly to the underlying progress tool. If ``mode="lazy"``,
        you can also pass ``poll_interval`` here.

    Returns
    -------
    ContextManager
        An object that yields a progress bar with an ``update()`` method.
    """

    def __new__(cls, mode: str, show: bool = True, **tqdm_kwargs: Any) -> object:
        if not show:
            return DummyPbar()

        if mode == "lazy":
            # Extract poll_interval if provided, otherwise default to 0.5s
            poll_interval = tqdm_kwargs.pop("poll_interval", 0.5)
            return CustomTqdmDaskCallback(poll_interval=poll_interval, **tqdm_kwargs)

        elif mode == "eager":
            return tqdm(**tqdm_kwargs)

        raise ValueError(f"Unknown progress mode: {mode}")
