# CTA and Dask
Here I test data streaming for CTA using the Dask.
First install dask distributed using
```
conda install dask distributed -c conda-forge
```

Then start a scheduler and a couple of worker tasks and run the `batch-cta.py` or `streaming-cta.py` scripts.
These scripts use different strategies to batch data before sending them to the worker queues.

