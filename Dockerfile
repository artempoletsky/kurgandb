FROM node:alpine
WORKDIR /usr/app

COPY . .
RUN --mount=type=cache,target=/root/.npm\
  npm i\
  && npx -p typescript tsc\
  && npm prune --omit=dev

EXPOSE 8080
CMD ["node", "./build/src/server.js"]