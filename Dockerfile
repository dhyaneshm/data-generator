### stage 0, build time
FROM adoptopenjdk/openjdk11 as build-stage
WORKDIR /usr/local/data-generator
COPY . .
RUN ./gradlew clean build

### stage 1, run time
FROM adoptopenjdk/openjdk11
WORKDIR /usr/local/data-generator
COPY --from=build-stage /usr/local/data-generator /usr/local/data-generator