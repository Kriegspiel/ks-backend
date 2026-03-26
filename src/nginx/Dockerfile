FROM nginx:1.27-alpine

ARG NGINX_PROFILE=dev

RUN rm -f /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/nginx.conf
COPY conf.d/kriegspiel.conf /etc/nginx/templates/kriegspiel-prod.conf
COPY conf.d/kriegspiel-dev.conf /etc/nginx/templates/kriegspiel-dev.conf
RUN if [ "$NGINX_PROFILE" = "prod" ]; then \
      cp /etc/nginx/templates/kriegspiel-prod.conf /etc/nginx/conf.d/kriegspiel.conf; \
    else \
      cp /etc/nginx/templates/kriegspiel-dev.conf /etc/nginx/conf.d/kriegspiel.conf; \
    fi

HEALTHCHECK --interval=10s --timeout=3s --retries=10 CMD wget -q -O /dev/null http://localhost/health || exit 1
