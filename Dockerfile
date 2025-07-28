# Build stage: Use Maven to compile and package the application
FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY target/car-rental-booking-service-0.0.1-SNAPSHOT.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
