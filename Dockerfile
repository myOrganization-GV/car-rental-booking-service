# Build stage: Use Maven to compile and package the application
FROM maven:3.8.5-openjdk-17 AS build
WORKDIR /app
COPY car-rental-booking-service/ car-rental-booking-service/
COPY car-rental-common/ car-rental-common/
RUN mvn -f car-rental-common/pom.xml clean install -DskipTests
RUN mvn -f car-rental-booking-service/pom.xml clean package -DskipTests


# Run stage: Using a lightweight JRE
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=build /app/car-rental-booking-service/target/car-rental-booking-service-0.0.1-SNAPSHOT.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]