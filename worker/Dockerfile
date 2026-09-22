# Sync worker Lambda. Build context: the repo root (it needs erp/ too).
#   docker build -f worker/Dockerfile -t relay-worker .
FROM public.ecr.aws/lambda/python:3.12

COPY worker/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt --target ${LAMBDA_TASK_ROOT}

COPY erp/ ${LAMBDA_TASK_ROOT}/erp/
COPY worker/ ${LAMBDA_TASK_ROOT}/worker/

# The base image's entrypoint runs this under the Runtime Interface Emulator
# when it isn't on AWS, so the same image answers invokes on port 8080 locally.
CMD ["worker.app.handler"]
