package com.gui.car_rental_booking_service.services;

import com.gui.car_rental_booking_service.entities.Booking;
import com.gui.car_rental_booking_service.enums.BookingStatus;
import com.gui.car_rental_booking_service.respositories.BookingRepository;
import com.gui.car_rental_booking_service.service.BookingService;
import com.gui.car_rental_common.commands.BookingCreationCommand;
import com.gui.car_rental_common.commands.CancelBookingCommand;
import com.gui.car_rental_common.dtos.BookingDto;
import com.gui.car_rental_common.events.booking.BookingCancellationFailedEvent;
import com.gui.car_rental_common.events.booking.BookingCancelledEvent;
import com.gui.car_rental_common.events.booking.BookingCreatedEvent;
import com.gui.car_rental_common.events.booking.BookingCreationFailedEvent;
import jakarta.persistence.EntityNotFoundException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.awt.print.Book;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class BookingServiceUnitTest {
    @Mock
    private BookingRepository bookingRepository;

    @Mock
    private KafkaTemplate<String, Object> kafkaTemplate;

    @InjectMocks
    private BookingService bookingService;

    private UUID bookingId;
    private Booking booking;
    private BookingDto bookingDto;
    private BookingCreationCommand bookingCreationCommand;

    @BeforeEach
    void setUp() {
        booking = new Booking();
        booking.setBookingId( UUID.randomUUID());
        booking.setCarId(UUID.randomUUID());
        booking.setRentalStartDate(LocalDateTime.now().plusDays(2));
        booking.setRentalEndDate(LocalDateTime.now().plusDays(5));
        booking.setUserId(UUID.randomUUID());
        booking.setUserEmail("test@example.com");

        bookingDto = new BookingDto();
        bookingDto.setBookingId(booking.getBookingId());
        bookingDto.setCarId(UUID.randomUUID());
        bookingDto.setRentalStartDate(LocalDateTime.now().plusDays(2));
        bookingDto.setRentalEndDate(LocalDateTime.now().plusDays(5));
        bookingDto.setPricePerDay(new BigDecimal("100.0"));
        bookingDto.setUserId(UUID.randomUUID());
        bookingDto.setEmail("command@example.com");

        bookingCreationCommand = new BookingCreationCommand(UUID.randomUUID(), bookingDto);
    }

    @Test
    void getBookingById_shouldReturnBooking_whenFound() {

        //arrange
        bookingId = booking.getBookingId();
        when(bookingRepository.findById(bookingId)).thenReturn(Optional.of(booking));
        //act
        Optional<Booking> result = bookingService.getBookingById(bookingId);
        //assert
        assertTrue(result.isPresent());
        assertEquals(bookingId, result.get().getBookingId());
        verify(bookingRepository, times(1)).findById(bookingId);
    }

    @Test
    void getBookingByID_shouldReturnEmptyOptional_whenNotFound(){
        //Arrange
        when(bookingRepository.findById(bookingId)).thenReturn(Optional.empty());

        //act
        Optional<Booking> booking = bookingService.getBookingById(bookingId);

        //assert
        assertFalse(booking.isPresent());
        verify(bookingRepository,times(1)).findById(bookingId);

    }

    @Test
    void getAllBookings_shouldReturnListOfBookings() {
        // Arrange
        List<Booking> bookings = Arrays.asList(booking, new Booking());
        when(bookingRepository.findAll()).thenReturn(bookings);

        // Act
        List<Booking> result = bookingService.getAllBookings();

        // Assert
        assertNotNull(result);
        assertEquals(2, result.size());
        assertEquals(booking,result.get(0));
        verify(bookingRepository, times(1)).findAll();
    }
    @Test
    void updateBooking_shouldUpdateAndReturnBooking_whenExists() {
        // Arrange
        when(bookingRepository.existsById(booking.getBookingId())).thenReturn(true);
        when(bookingRepository.save(booking)).thenReturn(booking);

        // Act
        Booking updatedBooking = bookingService.updateBooking(booking);

        // Assert
        assertNotNull(updatedBooking);
        assertEquals(booking.getBookingId(), updatedBooking.getBookingId());
        verify(bookingRepository, times(1)).existsById(booking.getBookingId());
        verify(bookingRepository, times(1)).save(booking);
    }

    @Test
    void updateBooking_shouldThrowException_whenBookingDoesNotExist() {
        // Arrange
        when(bookingRepository.existsById(booking.getBookingId())).thenReturn(false);

        // Act
        EntityNotFoundException exception = assertThrows(
                EntityNotFoundException.class,
                () -> bookingService.updateBooking(booking)
        );
        // assert
        assertEquals("Booking not found with ID: " + booking.getBookingId(), exception.getMessage());
        verify(bookingRepository, times(1)).existsById(booking.getBookingId());
        verify(bookingRepository, never()).save(any());
    }


    @Test
    void consumeBookingCreationCommand_shouldCreateBookingAndPublishEvent(){
        //arrange in the setup

        when(bookingRepository.save(any(Booking.class))).thenReturn(booking);
        when(kafkaTemplate.send(eq("booking-service-events"), any(BookingCreatedEvent.class))).thenReturn(CompletableFuture.completedFuture(null));
        //act
        Booking result = bookingService.consumeBookingCreationCommand(bookingCreationCommand);

        //Assert
        assertNotNull(result);
        assertEquals(booking.getBookingId(), result.getBookingId());

        ArgumentCaptor<Booking> argumentCaptor = ArgumentCaptor.forClass(Booking.class);
        verify(bookingRepository).save(argumentCaptor.capture());
        Booking bookingToSave = argumentCaptor.getValue();
        assertEquals(BookingStatus.PENDING, bookingToSave.getBookingStatus());

        LocalDateTime start = bookingCreationCommand.getBookingDto().getRentalStartDate();
        LocalDateTime end = bookingCreationCommand.getBookingDto().getRentalEndDate();
        long numberOfDays = Duration.between(start, end).toDays() + 1;

        assertEquals(bookingCreationCommand.getBookingDto().getPricePerDay().multiply(BigDecimal.valueOf(numberOfDays))
                ,bookingToSave.getTotalPrice());


        verify(bookingRepository, times(1)).save(any(Booking.class));
        verify(kafkaTemplate, times(1)).send(eq("booking-service-events"), any(BookingCreatedEvent.class));
    }


    @Test
    void consumeBookingCreationCommand_shouldPublishFailedEvent_whenStartDateTooSoon(){
        //Arrange
        bookingDto.setRentalStartDate(LocalDateTime.now().plusDays(1));
        bookingDto.setRentalEndDate(LocalDateTime.now().plusDays(5));

        when(kafkaTemplate.send(eq("booking-service-events"), any(BookingCreationFailedEvent.class))).thenReturn(CompletableFuture.completedFuture(null));
        //act
        Booking result = bookingService.consumeBookingCreationCommand(bookingCreationCommand);
        //assert
        assertNull(result);
        verify(bookingRepository, never()).save(any(Booking.class));
        ArgumentCaptor<BookingCreationFailedEvent> eventCaptor = ArgumentCaptor.forClass(BookingCreationFailedEvent.class);
        verify(kafkaTemplate, times(1)).send(eq("booking-service-events"), eventCaptor.capture());
        BookingCreationFailedEvent eventSent = eventCaptor.getValue();

        assertTrue(eventSent.getMessage().contains("Start date"), "Expected message to contain 'Start date'");
        assertTrue(eventSent.getMessage().toLowerCase().contains("must be at least one day after now"), "Expected message to explain the error");

    }

    @Test
    void consumeBookingCreationCommand_shouldPublishFailedEvent_whenEndDateTooLate() {
        // Arrange
        bookingDto.setRentalStartDate(LocalDateTime.now().plusDays(2));
        bookingDto.setRentalEndDate(bookingDto.getRentalStartDate().plusMonths(3));

        when(kafkaTemplate.send(eq("booking-service-events"), any(BookingCreationFailedEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Act
        Booking result = bookingService.consumeBookingCreationCommand(bookingCreationCommand);

        // Assert
        assertNull(result);
        verify(bookingRepository, never()).save(any(Booking.class));
        ArgumentCaptor<BookingCreationFailedEvent> eventCaptor = ArgumentCaptor.forClass(BookingCreationFailedEvent.class);
        verify(kafkaTemplate, times(1)).send(eq("booking-service-events"), eventCaptor.capture());

        BookingCreationFailedEvent eventSent = eventCaptor.getValue();
        assertTrue(eventSent.getMessage().contains("End date"), "Expected message to contain 'End date'");
        assertTrue(eventSent.getMessage().toLowerCase().contains("booking duration should not exceeds 2 months."), "Expected message to explain the error");
    }

    @Test
    void consumeBookingCreationCommand_shouldPublishFailedEvent_whenEndDateBeforeStart(){

        //Arrange
        bookingDto.setRentalStartDate(LocalDateTime.now().plusDays(10));
        bookingDto.setRentalEndDate(LocalDateTime.now().plusDays(5));

        when(kafkaTemplate.send(eq("booking-service-events"), any(BookingCreationFailedEvent.class))).thenReturn(CompletableFuture.completedFuture(null));
        //act
        Booking result = bookingService.consumeBookingCreationCommand(bookingCreationCommand);
        //assert
        assertNull(result);
        verify(bookingRepository, never()).save(any(Booking.class));
        ArgumentCaptor<BookingCreationFailedEvent> eventCaptor = ArgumentCaptor.forClass(BookingCreationFailedEvent.class);
        verify(kafkaTemplate, times(1)).send(eq("booking-service-events"), eventCaptor.capture());

        BookingCreationFailedEvent eventSent = eventCaptor.getValue();
        assertTrue(eventSent.getMessage().contains("End date"), "Expected message to contain 'End date'");
        assertTrue(eventSent.getMessage().toLowerCase().contains("after start date"), "Expected message to explain the error");
    }

    @Test
    void consumeCancelBookingCommand_shouldCancelBookingAndSendEvent() {
        // Arrange
        booking.setBookingStatus(BookingStatus.PENDING);

        CancelBookingCommand command = new CancelBookingCommand(UUID.randomUUID(), bookingDto);
        when(bookingRepository.findById(bookingDto.getBookingId())).thenReturn(Optional.of(booking));
        when(bookingRepository.save(any(Booking.class))).thenReturn(booking);
        when(kafkaTemplate.send(eq("booking-service-events"), any(BookingCancelledEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Act
        bookingService.consumeCancelBookingCommand(command);

        // Assert
        ArgumentCaptor<Booking> bookingCaptor = ArgumentCaptor.forClass(Booking.class);
        verify(bookingRepository).save(bookingCaptor.capture());
        assertEquals(BookingStatus.CANCELLED, bookingCaptor.getValue().getBookingStatus());

        ArgumentCaptor<BookingCancelledEvent> eventCaptor = ArgumentCaptor.forClass(BookingCancelledEvent.class);
        verify(kafkaTemplate).send(eq("booking-service-events"), eventCaptor.capture());

        BookingCancelledEvent event = eventCaptor.getValue();
        assertEquals(command.getSagaTransactionId(), event.getSagaTransactionId());
        assertEquals(command.getBookingDto(), event.getBookingDto());

        verify(kafkaTemplate, times(1)).send(eq("booking-service-events"), any(BookingCancelledEvent.class));
    }

    @Test
    void consumeCancelBookingCommand_shouldSendFailureEvent_whenBookingNotFound(){
        // Arrange
        CancelBookingCommand command = new CancelBookingCommand(UUID.randomUUID(), bookingDto);
        when(bookingRepository.findById(bookingDto.getBookingId())).thenReturn(Optional.empty());
        when(kafkaTemplate.send(eq("booking-service-events"), any(BookingCancellationFailedEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(null));

        // Act
        bookingService.consumeCancelBookingCommand(command);

        // Assert
        verify(bookingRepository, never()).save(any());

        ArgumentCaptor<BookingCancellationFailedEvent> eventCaptor = ArgumentCaptor.forClass(BookingCancellationFailedEvent.class);
        verify(kafkaTemplate).send(eq("booking-service-events"), eventCaptor.capture());

        BookingCancellationFailedEvent failedEvent = eventCaptor.getValue();
        assertEquals(command.getSagaTransactionId(), failedEvent.getSagaTransactionId());
        assertEquals(command.getBookingDto(), failedEvent.getBookingDto());
        assertTrue(failedEvent.getMessage().toLowerCase().contains("no value present"));
    }

}
