FROM node:12.18.1-alpine
WORKDIR /usr/src/app
COPY package*.json ./
RUN npm install --only=production
COPY ./src ./src
COPY ./videos ./videos
COPY oas.yaml ./
CMD npm start