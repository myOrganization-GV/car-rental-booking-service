package com.gui.car_rental_booking_service.services;


import com.gui.car_rental_booking_service.entities.Booking;
import com.gui.car_rental_booking_service.enums.BookingStatus;
import com.gui.car_rental_booking_service.respositories.BookingRepository;
import com.gui.car_rental_common.commands.BookingCreationCommand;
import com.gui.car_rental_common.commands.CancelBookingCommand;
import com.gui.car_rental_common.dtos.BookingDto;
import com.gui.car_rental_common.events.booking.BookingCancellationFailedEvent;
import com.gui.car_rental_common.events.booking.BookingCancelledEvent;
import com.gui.car_rental_common.events.booking.BookingCreatedEvent;
import com.gui.car_rental_common.events.booking.BookingCreationFailedEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.ConfluentKafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@Testcontainers
class BookingServiceIntegrationTest {

    @Container
    private static final ConfluentKafkaContainer kafka = new ConfluentKafkaContainer(
            DockerImageName.parse("confluentinc/cp-kafka:7.5.3"));

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", kafka::getBootstrapServers);
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;
    @Autowired
    private BookingRepository bookingRepository;

    private Consumer<String, Object> eventConsumer;
    private final String COMMAND_TOPIC = "rental-saga-booking-commands";
    private final String EVENT_TOPIC = "booking-service-events";

    @BeforeEach
    void setUp() {
        bookingRepository.deleteAll();
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(JsonDeserializer.TRUSTED_PACKAGES, "*");

        eventConsumer = new KafkaConsumer<>(consumerProps);
        eventConsumer.subscribe(Collections.singletonList(EVENT_TOPIC));
    }

    @AfterEach
    void tearDown() {
        if (eventConsumer != null) {
            eventConsumer.close();
        }
    }

    @Test
    void testBookingCreationCommand_Success() {

        var bookingDto = new BookingDto();
        bookingDto.setCarId(UUID.randomUUID());
        bookingDto.setUserId(UUID.randomUUID());
        bookingDto.setEmail("test@example.com");
        bookingDto.setPricePerDay(BigDecimal.valueOf(100));
        bookingDto.setRentalStartDate(LocalDateTime.now().plusDays(2));
        bookingDto.setRentalEndDate(LocalDateTime.now().plusDays(4));

        UUID sagaId = UUID.randomUUID();
        BookingCreationCommand cmd  = new BookingCreationCommand(sagaId, bookingDto);

        kafkaTemplate.send(COMMAND_TOPIC, cmd);

        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    assertThat(bookingRepository.findAll()).hasSize(1);
                });


        ConsumerRecord<String, Object> record =
                KafkaTestUtils.getSingleRecord(eventConsumer, EVENT_TOPIC);

        assertThat(record.value())
                .isInstanceOf(BookingCreatedEvent.class);

        BookingCreatedEvent event = (BookingCreatedEvent) record.value();
        assertThat(event.getSagaTransactionId()).isEqualTo(sagaId);
        assertThat(event.getBookingDto().getBookingId()).isNotNull();
        assertThat(event.getBookingDto().getAmount())
                .isEqualByComparingTo(BigDecimal.valueOf(300));

        UUID persistedId = event.getBookingDto().getBookingId();
        Booking persisted = bookingRepository.findById(persistedId).orElseThrow();
        assertThat(persisted.getBookingStatus()).isEqualTo(BookingStatus.PENDING);
 }

    @Test
    void testBookingCreationCommand_Failure(){
        var bookingDto = new BookingDto();
        bookingDto.setCarId(UUID.randomUUID());
        bookingDto.setUserId(UUID.randomUUID());
        bookingDto.setPricePerDay(BigDecimal.valueOf(50));
        LocalDateTime start = LocalDateTime.now().plusDays(1);
        bookingDto.setRentalStartDate(start);
        bookingDto.setRentalEndDate(start);
        UUID sagaId = UUID.randomUUID();
        BookingCreationCommand cmd = new BookingCreationCommand(sagaId, bookingDto);

        kafkaTemplate.send(COMMAND_TOPIC, cmd);

        ConsumerRecord<String, Object> failureRecord =
                KafkaTestUtils.getSingleRecord(eventConsumer, EVENT_TOPIC, Duration.ofSeconds(10L));

        assertThat(failureRecord.value())
                .isInstanceOf(BookingCreationFailedEvent.class);

        BookingCreationFailedEvent failedEvent = (BookingCreationFailedEvent) failureRecord.value();
        assertThat(failedEvent.getSagaTransactionId()).isEqualTo(sagaId);

        List<Booking> all = bookingRepository.findAll();
        assertThat(all).isEmpty();

    }

    @Test
    void testCancelBookingCommand_Success(){
        Booking booking = new Booking();
        booking.setCarId(UUID.randomUUID());
        booking.setRentalStartDate(LocalDateTime.now().plusDays(2));
        booking.setRentalEndDate(LocalDateTime.now().plusDays(5));
        booking.setBookingStatus(BookingStatus.PENDING);
        booking.setTotalPrice(new BigDecimal("300.0"));
        booking.setUserId(UUID.randomUUID());
        booking.setUserEmail("test@example.com");

        Booking saved = bookingRepository.saveAndFlush(booking);

        BookingDto dto = new BookingDto();
        dto.setBookingId(saved.getBookingId());
        dto.setCarId(saved.getCarId());
        dto.setUserId(saved.getUserId());
        dto.setEmail(saved.getUserEmail());
        dto.setRentalStartDate(saved.getRentalStartDate());
        dto.setRentalEndDate(saved.getRentalEndDate());
        dto.setAmount(saved.getTotalPrice());

        UUID sagaId = UUID.randomUUID();
        CancelBookingCommand cancelCmd = new CancelBookingCommand(sagaId, dto);
        kafkaTemplate.send(COMMAND_TOPIC, cancelCmd);

        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    Booking updated = bookingRepository.findById(saved.getBookingId()).orElseThrow();
                    assertThat(updated.getBookingStatus()).isEqualTo(BookingStatus.CANCELLED);
                });

        ConsumerRecord<String, Object> rec =
                KafkaTestUtils.getSingleRecord(eventConsumer, EVENT_TOPIC);

        assertThat(rec.value()).isInstanceOf(BookingCancelledEvent.class);

        BookingCancelledEvent event = (BookingCancelledEvent) rec.value();
        assertThat(event.getSagaTransactionId()).isEqualTo(sagaId);
        assertThat(event.getBookingDto().getBookingId()).isEqualTo(saved.getBookingId());
    }


    @Test
    void testCancelBookingCreationCommand_Failure(){
        BookingDto dto = new BookingDto();
        dto.setBookingId(UUID.randomUUID());

        UUID sagaId = UUID.randomUUID();

        CancelBookingCommand cancelCmd = new CancelBookingCommand(sagaId, dto);
        kafkaTemplate.send(COMMAND_TOPIC, cancelCmd);
        Awaitility.await()
                .atMost(Duration.ofSeconds(10))
                .untilAsserted(() -> {
                    ConsumerRecord<String, Object> record = KafkaTestUtils.getSingleRecord(eventConsumer, EVENT_TOPIC);
                    assertThat(record.value()).isInstanceOf(BookingCancellationFailedEvent.class);

                    BookingCancellationFailedEvent failedEvent = (BookingCancellationFailedEvent) record.value();

                    assertThat(failedEvent.getSagaTransactionId()).isEqualTo(sagaId);
                    assertThat(failedEvent.getMessage()).contains("No value present");
                });
    }
}