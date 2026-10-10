FROM node:22-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
# The browser talks to the API instances directly, so these are host-reachable URLs.
ARG VITE_SEED_URLS=http://localhost:4001,http://localhost:4002,http://localhost:4003
ENV VITE_SEED_URLS=$VITE_SEED_URLS
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/dist /usr/share/nginx/html
EXPOSE 80
