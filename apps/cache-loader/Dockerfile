FROM node:20-alpine
WORKDIR /app

COPY package*.json ./
COPY apps/cache-loader/package.json ./apps/cache-loader/package.json
RUN npm install --workspaces --include-workspace-root

COPY apps/cache-loader ./apps/cache-loader

WORKDIR /app/apps/cache-loader
EXPOSE 3010
CMD ["npm", "start"]
