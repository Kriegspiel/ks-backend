FROM nginx:1.27-alpine

RUN rm -f /etc/nginx/conf.d/default.conf
COPY nginx.conf /etc/nginx/nginx.conf
COPY conf.d /etc/nginx/conf.d

HEALTHCHECK --interval=10s --timeout=3s --retries=10 CMD wget -q -O /dev/null http://localhost/health || exit 1
