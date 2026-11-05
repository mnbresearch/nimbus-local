FROM node:22-alpine
WORKDIR /app
COPY . .
EXPOSE 4566
ENV NIMBUS_HOST=0.0.0.0
HEALTHCHECK --interval=10s --timeout=2s CMD wget -qO- http://127.0.0.1:4566/_nimbus/health || exit 1
CMD ["node", "nimbus.mjs"]
