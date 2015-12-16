FROM docker:1.9-dind

COPY . cookiemonster
WORKDIR cookiemonster

RUN chmod +x docker/test/dind/entrypoint.sh
CMD ["./docker/test/dind/entrypoint.sh"]