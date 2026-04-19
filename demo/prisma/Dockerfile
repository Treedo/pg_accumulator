FROM node:20-bullseye-slim AS builder

WORKDIR /app

# Copy prisma-accumulator package source
COPY packages/prisma-accumulator /packages/prisma-accumulator
WORKDIR /packages/prisma-accumulator
RUN npm install && npm run build

# Build demo app
WORKDIR /app/demo
COPY demo/prisma/package.json demo/prisma/package-lock.json* ./
RUN npm install
COPY demo/prisma/tsconfig.json ./
COPY demo/prisma/prisma ./prisma/
RUN npx prisma generate
COPY demo/prisma/src ./src/
RUN npm run build

# ---- runtime stage ----
FROM node:20-bullseye-slim

RUN apt-get update && apt-get install -y libssl1.1 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /packages/prisma-accumulator /packages/prisma-accumulator
COPY --from=builder /app/demo/package.json ./
COPY --from=builder /app/demo/node_modules ./node_modules/
COPY --from=builder /app/demo/dist ./dist/
COPY demo/prisma/public ./public/

EXPOSE 3000
CMD ["node", "dist/index.js"]
