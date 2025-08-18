FROM eclipse-temurin:17-jre as base
WORKDIR /app
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

FROM gradle:8.7.0-jdk17 as build
WORKDIR /workspace
COPY . .
RUN gradle clean bootJar -x test --no-daemon

FROM base as runtime
USER appuser
WORKDIR /app
COPY --from=build /workspace/build/libs/*-SNAPSHOT.jar app.jar
ENV JAVA_OPTS="-XX:MaxRAMPercentage=75 -XX:+UseG1GC -Dfile.encoding=UTF-8"
EXPOSE 8083
ENTRYPOINT ["sh","-c","java $JAVA_OPTS -jar app.jar"]


