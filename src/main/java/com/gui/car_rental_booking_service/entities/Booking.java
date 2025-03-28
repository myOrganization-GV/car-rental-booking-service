package com.gui.car_rental_booking_service.entities;

import com.gui.car_rental_booking_service.enums.BookingStatus;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.UUID;

@Entity
@Table(name = "bookings")
public class Booking {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID bookingId;
    @NotNull
    private UUID carId;

    private UUID userId;

    private String userName;

    private LocalDateTime rentalStartDate;

    private LocalDateTime rentalEndDate;
    @Enumerated(EnumType.STRING)
    private BookingStatus bookingStatus;

    private BigDecimal totalPrice;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private LocalDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private LocalDateTime updatedAt;

    public Booking() {
    }

    public Booking(UUID bookingId, UUID carId, UUID userId, String userName, LocalDateTime rentalStartDate, LocalDateTime rentalEndDate,
                   BookingStatus bookingStatus,BigDecimal totalPrice, LocalDateTime createdAt,
                   LocalDateTime updatedAt) {
        this.bookingId = bookingId;
        this.carId = carId;
        this.userId = userId;
        this.userName = userName;
        this.rentalStartDate = rentalStartDate;
        this.rentalEndDate = rentalEndDate;
        this.bookingStatus = bookingStatus;
        this.totalPrice = totalPrice;
        this.createdAt = createdAt;
        this.updatedAt = updatedAt;
    }

    public BigDecimal getTotalPrice() {
        return totalPrice;
    }

    public void setTotalPrice(BigDecimal totalPrice) {
        this.totalPrice = totalPrice;
    }

    public UUID getBookingId() {
        return bookingId;
    }

    public void setBookingId(UUID bookingId) {
        this.bookingId = bookingId;
    }

    public UUID getCarId() {
        return carId;
    }

    public void setCarId(UUID carId) {
        this.carId = carId;
    }

    public UUID getUserId() {
        return userId;
    }

    public void setUserId(UUID userId) {
        this.userId = userId;
    }

    public String getUserName() {
        return userName;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public LocalDateTime getRentalStartDate() {
        return rentalStartDate;
    }

    public void setRentalStartDate(LocalDateTime rentalStartDate) {
        this.rentalStartDate = rentalStartDate;
    }

    public LocalDateTime getRentalEndDate() {
        return rentalEndDate;
    }

    public void setRentalEndDate(LocalDateTime rentalEndDate) {
        this.rentalEndDate = rentalEndDate;
    }

    public BookingStatus getBookingStatus() {
        return bookingStatus;
    }

    public void setBookingStatus(BookingStatus bookingStatus) {
        this.bookingStatus = bookingStatus;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }
    public void calculateTotalPrice(BigDecimal pricePerDay) {
        long daysBetween = ChronoUnit.DAYS.between(rentalStartDate, rentalEndDate) + 1;
        this.totalPrice = pricePerDay.multiply(BigDecimal.valueOf(daysBetween));
    }
}
