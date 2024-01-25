package com.scottlogic.pod.spark.playground.junit;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import com.scottlogic.pod.spark.playground.CSVReader;

public class WordCountMockedTests {

    private CSVReader CSVReader;

    @Mock
    private SparkSession mockSparkSession;

    @Mock
    private JavaSparkContext mockJavaSparkContext;


    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        CSVReader = new CSVReader(mockSparkSession, mockJavaSparkContext);
    }

    @Test
    public void testMockedCountCsv() {
        String filePath = "data/example-words.txt";

        DataFrameReader mockDataFrameReader = mock(DataFrameReader.class);

        @SuppressWarnings("unchecked")
        Dataset<Row> mockDataSet = mock(Dataset.class);

        when(mockSparkSession.read()).thenReturn(mockDataFrameReader);
        when(mockDataFrameReader.csv(filePath)).thenReturn(mockDataSet);
        when(mockDataSet.count()).thenReturn(new Long(188));

        
        long result = CSVReader.countCsv(filePath);

        verify(mockSparkSession, times(1)).read();
        verify(mockDataFrameReader, times(1)).csv(filePath);
        verify(mockDataSet, times(1)).count();

        assertEquals(188, result);
    }
}
