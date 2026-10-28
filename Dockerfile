FROM oven/bun:1-alpine AS base
WORKDIR /app

# Instala dependências com cache de layer
FROM base AS deps
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile

# Stage final de execução
FROM base AS runner
WORKDIR /app

ENV NODE_ENV=production
ENV PORT=3000
ENV AUTO_MIGRATE=true

COPY --from=deps /app/node_modules ./node_modules
COPY package.json tsconfig.json bunfig.toml ./
COPY src ./src
COPY scripts ./scripts

EXPOSE 3000

STOPSIGNAL SIGTERM

HEALTHCHECK --interval=10s --timeout=5s --retries=5 --start-period=10s \
  CMD wget -qO- http://localhost:3000/health/live || exit 1

CMD ["bun", "run", "src/main.ts"]
