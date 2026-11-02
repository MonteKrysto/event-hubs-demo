FROM node:24-bookworm-slim AS web-build
WORKDIR /app
COPY package.json package-lock.json ./
COPY scripts ./scripts
RUN npm ci --no-audit --no-fund
COPY web ./web
RUN npm run build:web

FROM node:24-bookworm-slim
WORKDIR /app
COPY package.json package-lock.json ./
COPY scripts ./scripts
RUN npm ci --omit=dev --no-audit --no-fund
COPY src ./src
COPY db ./db
COPY schemas ./schemas
COPY fixtures ./fixtures
COPY tests ./tests
COPY web/src/learning.ts web/src/types.ts ./web/src/
COPY --from=web-build /app/dist/web ./dist/web
USER node
ENTRYPOINT ["node", "src/cli.js"]
CMD ["--help"]
