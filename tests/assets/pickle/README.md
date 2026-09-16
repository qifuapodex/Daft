# Execution-config and distributed-plan pickle fixtures

These JSON files contain base64-encoded, standard-library `pickle.dumps` output
from the released Linux x86_64 wheels on Python 3.11.11:

| Version | Source commit | Config/plan factory |
| --- | --- | --- |
| `0.7.24+apodex.6` | `83898cdce391c07406e33b4ac3662660208a0015` | `_from_serialized_shuffle_aqe_v1` |
| `0.7.24+apodex.7` | `6a845f7a0ca48dcbe09003ec8435aa874734ed5b` | `_from_serialized_shuffle_eio_v2` |

Generated on 2026-09-16 from the installed release wheels, using the same helper
as regression report A7-005. Run this script with each release's interpreter and
`config` or `plan` as the argument, outside a Daft source checkout:

```python
import base64
import json
import pickle
import sys

import daft

daft.set_runner_native()
obj = daft.context.get_context().daft_execution_config
if sys.argv[1] == "plan":
    from daft.daft import DistributedPhysicalPlan

    frame = daft.range(0, 16, partitions=2).repartition(3, "id")
    obj = DistributedPhysicalPlan.from_logical_plan_builder(
        frame._builder.optimize(obj)._builder, "pickle-cross", obj
    )
print(json.dumps({
    "source_version": daft.__version__,
    "factory": obj.__reduce__()[0].__name__,
    "payload": base64.b64encode(pickle.dumps(obj)).decode(),
}, indent=2))
```

No Ray cluster or query execution is needed. The fixtures check rejection of
real `.6` pickles and continued loading of released `.7` pickles. Do not regenerate
the `.6` fixtures with the current factory or relabel `.7` bytes as `.6` data.
