package com.scottlogic.pod.spark.playground.junit;

import static org.junit.Assert.assertEquals;

import java.io.IOException;

import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.scottlogic.pod.spark.playground.WordCount;
import com.scottlogic.pod.spark.playground.config.SparkIntegrationTestBase;

public class WordCountIntegrationTest extends SparkIntegrationTestBase {

    private WordCount wordCount;

    @Before
    public void setUp() {
        wordCount = new WordCount(getSpark(), getJsc());
    }

    @After
    public void tearDown() {
        stopSparkContext();
    }
    
    @Test
    public void testCountDistinctWords() throws IOException {
        String filePath = "data/example-words.txt";
        long result = wordCount.countDistinctWords(filePath);
        assertEquals(188, result);
    }

    @Test
    public void testCountDistinctWordsWithFewerWords() throws IOException {
        String filePath = "data/example-words-fewer-words.txt";
        long result = wordCount.countDistinctWords(filePath);
        assertEquals(181, result);
    }

    @Test
    public void testCountDistinctWordsWithEmptyFile() throws IOException {
        String filePath = "src/test/resources/com/scottlogic/pod/spark/playground/junit/empty-file.txt";
        long result = wordCount.countDistinctWords(filePath);
        assertEquals(0, result);
    }

    @Test
    public void testCountDistinctWordsWithNullFile() throws IOException {
        String filePath = null;
        Assertions.assertThatThrownBy(() -> {
            wordCount.countDistinctWords(filePath);
        }).isInstanceOf(NullPointerException.class);
    }
}
