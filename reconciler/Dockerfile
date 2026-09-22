# Reconciler Lambda. Build context: the repo root. It shares erp/ and the
# worker's validator, so a deal it re-enqueues passes the same checks.
FROM public.ecr.aws/lambda/python:3.12

COPY worker/requirements.txt ${LAMBDA_TASK_ROOT}/
RUN pip install --no-cache-dir -r ${LAMBDA_TASK_ROOT}/requirements.txt --target ${LAMBDA_TASK_ROOT}

COPY erp/ ${LAMBDA_TASK_ROOT}/erp/
COPY worker/ ${LAMBDA_TASK_ROOT}/worker/
COPY reconciler/ ${LAMBDA_TASK_ROOT}/reconciler/

ENV RELAY_SERVICE=reconciler
CMD ["reconciler.app.handler"]
