package com.scottlogic.pod.spark.playground.junit;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ WordCountIntegrationTest.class, WordCountMockedTests.class })
public class WordCountJunitSuiteRunner {
}
