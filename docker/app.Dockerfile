# One image recipe for both apps: --build-arg MODULE=flowforge-orchestrator | flowforge-worker.
# Stage 1 builds the module (and what it depends on) with the Maven wrapper; stage 2 runs the jar on a JRE as non-root.

FROM eclipse-temurin:21-jdk AS build
WORKDIR /src
COPY .mvn/ .mvn/
COPY mvnw pom.xml ./
COPY config/ config/
COPY flowforge-core/ flowforge-core/
COPY flowforge-runtime/ flowforge-runtime/
COPY flowforge-orchestrator/ flowforge-orchestrator/
COPY flowforge-worker/ flowforge-worker/
COPY flowforge-system-tests/pom.xml flowforge-system-tests/pom.xml
ARG MODULE
RUN ./mvnw -B -q -pl ${MODULE} -am -DskipTests package \
 && cp ${MODULE}/target/${MODULE}-*-exec.jar /app.jar

FROM eclipse-temurin:21-jre
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/* \
 && useradd --system --uid 10001 flowforge
COPY --from=build /app.jar /app/app.jar
USER flowforge
ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-XX:+ExitOnOutOfMemoryError", "-jar", "/app/app.jar"]
