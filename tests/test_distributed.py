from __future__ import annotations

import pytest

from ydana.utils.functions import make_dask_sched_kwargs


def test_make_dask_sched_kwargs_ncores_1_is_synchronous() -> None:
    kwargs, label = make_dask_sched_kwargs(ncores=1)
    assert kwargs == {"scheduler": "synchronous"}
    assert label == "synchronous"


def test_make_dask_sched_kwargs_ncores_minus_1_is_default() -> None:
    kwargs, label = make_dask_sched_kwargs(ncores=-1)
    assert kwargs == {}
    assert "default" in label


def test_make_dask_sched_kwargs_uses_distributed_client_when_active() -> None:
    distributed = pytest.importorskip("dask.distributed")

    with distributed.LocalCluster(n_workers=1, threads_per_worker=1, processes=False) as cluster:
        with distributed.Client(cluster):
            kwargs, label = make_dask_sched_kwargs(ncores=4)

    assert kwargs == {}
    assert label == "distributed"
