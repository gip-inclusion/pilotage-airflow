import itertools
import sys


if sys.version_info >= (3, 12):
    from itertools import batched
else:

    def batched(iterable, n, *, strict=False):
        """Batch data from the iterable into tuples of length n. The last batch may be shorter than n.

        Source: Python stdlib, added in version 3.12.
        """
        # batched('ABCDEFG', 3) → ABC DEF G
        if n < 1:
            raise ValueError("n must be at least one")
        iterator = iter(iterable)
        while batch := tuple(itertools.islice(iterator, n)):
            if strict and len(batch) != n:
                raise ValueError("batched(): incomplete batch")
            yield batch
