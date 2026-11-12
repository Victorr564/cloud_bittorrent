# ---- build stage ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /app

# Copy only the pom first so dependency downloads are cached
# and only re-run when pom.xml changes
COPY pom.xml .
RUN mvn -B -q dependency:go-offline

COPY src ./src
RUN mvn -B -q package -DskipTests

# ---- runtime stage ----
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app

COPY --from=build /app/target/BittorentService-1.0-SNAPSHOT.jar BittorrentService.jar

ENTRYPOINT ["java", "-jar", "BittorrentService.jar"]
