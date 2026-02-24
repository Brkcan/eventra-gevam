FROM node:20-alpine

WORKDIR /app

COPY package.json package-lock.json ./
COPY apps ./apps
COPY services ./services

RUN npm ci --omit=dev

WORKDIR /app/services/rule-engine

CMD ["node", "src/index.js"]

