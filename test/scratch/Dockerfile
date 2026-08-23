# The scratch proof itself: no shell, no zoneinfo, no network. The binary
# carries everything it needs, including the embedded tzdata. Built by
# make test-scratch, which runs docker build from the repository root so the
# payload is picked up from bin/scratch.
FROM scratch
COPY bin/scratch/paceq-scratch /paceq-scratch
ENV TZ=Europe/Oslo
ENTRYPOINT ["/paceq-scratch"]
