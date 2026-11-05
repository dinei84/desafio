FROM oven/bun:1.3.14
WORKDIR /app
COPY package.json bun.lock ./
RUN bun install --frozen-lockfile
COPY tsconfig.json ./
COPY src ./src
COPY scripts ./scripts
COPY test ./test
USER bun
EXPOSE 3000
CMD ["bun", "run", "start"]
