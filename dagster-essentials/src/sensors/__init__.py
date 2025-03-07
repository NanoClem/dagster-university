import json
from pathlib import Path

import dagster as dg

from ..jobs import adhoc_request_job


@dg.sensor(
    job=adhoc_request_job,
    minimum_interval_seconds=5,
)
def adhoc_request_sensor(context: dg.SensorEvaluationContext) -> dg.SensorResult:
    requests_path = Path(__file__).parents[2] / "data" / "requests"
    prev_state = json.loads(context.cursor) if context.cursor else {}
    curr_state = {}
    runs_to_request = []

    for path in requests_path.iterdir():
        filename = path.name

        if not (path.is_file() and filename.endswith(".json")):
            continue

        last_modified = path.stat().st_mtime
        curr_state[filename] = last_modified

        if filename not in prev_state or prev_state[filename] != last_modified:
            with open(path, "r") as req_file:
                req_config = json.load(req_file)
                runs_to_request.append(
                    dg.RunRequest(
                        run_key=f"adhoc_request_{filename}_{last_modified}",
                        run_config={
                            "ops": {
                                "adhoc_request": {
                                    "config": {"filename": filename, **req_config}
                                }
                            }
                        },
                    )
                )

    return dg.SensorResult(
        run_requests=runs_to_request,
        cursor=json.dumps(curr_state),
    )
