# Stage 7 spike: serverless hub

Throwaway prototype that answered Q7-Q11 in PLAN.md. Results are recorded
there under "Результаты этапа 7". Delete this directory once stage 9
repeats its checks.

```bash
export AWS_PROFILE=<profile> AWS_REGION=eu-central-1
export SPIKE_STATE=<file outside the repo for URLs and raw tokens>

spike/hub/build.sh
sam deploy --template-file spike/hub/template.yaml --stack-name agent-bus-spike \
    --s3-bucket <artifact bucket> --capabilities CAPABILITY_IAM \
    --tags project=agent-bus purpose=spike

cd spike/hub/probes
uv run --no-project --with 'boto3[crt]' python seed.py agent-bus-spike alice bob carol dave erin frank
uv run --no-project --with httpx --with websockets python idle.py 780          # Q7
uv run --no-project --with 'boto3[crt]' --with httpx --with websockets python auth.py frank   # Q8
uv run --no-project --with httpx --with websockets python sleeper.py 780 post bob    # Q9
uv run --no-project --with httpx --with websockets python sleeper.py 780 quiet erin  # Q9
PYTHONPATH=../../../src uv run --no-project --with httpx --with pydantic python race.py 10 20  # Q11
uv run --no-project --with httpx --with websockets python push.py 20
```

Probes that hold sockets must not share agents with probes that send
messages: every message pushes `{seq}` to the parties' sockets, and a push
counts as activity on the connection.
