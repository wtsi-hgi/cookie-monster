FROM docker:1.9-dind

COPY . cookiemonster
WORKDIR cookiemonster

RUN chmod +x docker/tests/entrypoint.sh
CMD ["./docker/tests/entrypoint.sh"]