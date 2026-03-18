"""User-facing analysis entry points exposed by the ydana CLI."""

__all__ = ["dump_events", "fill_histos", "merge_histos", "merge_roots"]


def __getattr__(name: str) -> object:
    if name == "dump_events":
        from .events_dumper import dump_events

        return dump_events

    if name == "fill_histos":
        from .histos_filler import fill_histos

        return fill_histos

    if name == "merge_histos":
        from .histos_merger import merge_histos

        return merge_histos

    if name == "merge_roots":
        from .roots_merger import merge_roots

        return merge_roots

    raise AttributeError(f"module 'ydana.analysis' has no attribute {name!r}")
