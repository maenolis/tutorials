package com.baeldung.logging;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.group;
import static org.springframework.data.mongodb.core.aggregation.Aggregation.newAggregation;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.core.query.Update.update;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.GroupOperation;
import org.springframework.test.context.TestPropertySource;

import com.baeldung.logging.model.Book;

@SpringBootTest(classes = SpringBootLoggingApplication.class)
@TestPropertySource(properties = { "logging.level.org.springframework.data.mongodb.core.MongoTemplate=WARN" }, value = "/embedded.properties")
public class LoggingUnitTest {

    private @Autowired MongoTemplate mongoTemplate;

    @BeforeEach
    void setup() throws Exception {
        mongoTemplate.dropCollection("book");
    }

    @Test
    void whenInsertDocument_thenFindByIdOk() {
        Book book = new Book();
        book.setBookName("Book");
        book.setAuthorName("Author");

        mongoTemplate.insert(book);

        assertThat(mongoTemplate.findById(book.getId(), Book.class)).isEqualTo(book);
    }

    @Test
    void givenExistingDocument_whenUpdateDocument_thenFieldIsUpdatedOk() {
        Book book = new Book();
        book.setBookName("Book");
        book.setAuthorName("Author");

        mongoTemplate.insert(book);

        String authorNameUpdate = "AuthorNameUpdate";

        book.setAuthorName(authorNameUpdate);
        mongoTemplate.updateFirst(query(where("bookName").is("Book")), update("authorName", authorNameUpdate), Book.class);

        assertThat(mongoTemplate.findById(book.getId(), Book.class)).extracting(Book::getAuthorName)
            .isEqualTo(authorNameUpdate);
    }

    @Test
    void whenInsertMultipleDocuments_thenFindAllOk() {
        Book book = new Book();
        book.setBookName("Book");
        book.setAuthorName("Author");

        Book book1 = new Book();
        book1.setBookName("Book1");
        book1.setAuthorName("Author1");

        mongoTemplate.insert(Arrays.asList(book, book1), Book.class);

        assertThat(mongoTemplate.findAll(Book.class)
            .size()).isEqualTo(2);
    }

    @Test
    void givenExistingDocument_whenRemoveDocument_thenDocumentIsDeleted() {
        Book book = new Book();
        book.setBookName("Book");
        book.setAuthorName("Author");

        mongoTemplate.insert(book);

        mongoTemplate.remove(book);

        assertThat(mongoTemplate.findAll(Book.class)
            .size()).isEqualTo(0);
    }

    @Test
    void whenAggregateByField_thenGroupByCountIsOk() {
        Book book = new Book();
        book.setBookName("Book");
        book.setAuthorName("Author");

        Book book1 = new Book();
        book1.setBookName("Book1");
        book1.setAuthorName("Author");

        Book book2 = new Book();
        book2.setBookName("Book2");
        book2.setAuthorName("Author");

        mongoTemplate.insert(Arrays.asList(book, book1, book2), Book.class);

        GroupOperation groupByAuthor = group("authorName").count()
            .as("authCount");

        Aggregation aggregation = newAggregation(groupByAuthor);

        AggregationResults<GroupByAuthor> aggregationResults = mongoTemplate.aggregate(aggregation, "book", GroupByAuthor.class);

        List<GroupByAuthor> groupByAuthorList = StreamSupport.stream(aggregationResults.spliterator(), false)
            .collect(Collectors.toList());

        assertThat(groupByAuthorList.stream()
            .filter(l -> l.getAuthorName()
                .equals("Author"))
            .findFirst()
            .orElse(null)).extracting(GroupByAuthor::getAuthCount)
            .isEqualTo(3);
    }

}
