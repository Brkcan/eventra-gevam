FROM node:20-alpine AS build

WORKDIR /app

COPY package.json package-lock.json ./
COPY apps ./apps
COPY services ./services

RUN npm ci

ARG VITE_API_BASE_URL=http://localhost:3001
ENV VITE_API_BASE_URL=${VITE_API_BASE_URL}

RUN npm --workspace @eventra/frontend run build

FROM nginx:1.27-alpine

COPY apps/frontend/nginx.conf /etc/nginx/conf.d/default.conf
COPY --from=build /app/apps/frontend/dist /usr/share/nginx/html

EXPOSE 80

