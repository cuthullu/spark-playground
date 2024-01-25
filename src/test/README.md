## Running Tests

This project uses JUnit for testing. You can run the tests either using the start buttons in your IDE's test runner classes or through Maven.

### Running Tests in IDE (vsCode/IntelliJ):

1. Open your IDE.
2. Navigate to `src/test/java/com/scottlogic/pod/spark/playground` directory
3. Within the 'cucumber' and 'junit' folders are Java classes ending in Runner.
4. Find and click the "Run" button or use the appropriate shortcut to run the tests.

### Running Tests with Maven:

This project uses Maven as a build tool. To run the tests using Maven, follow these steps:

1. Open a terminal or command prompt.

2. Navigate to the root directory of the project where the `pom.xml` file is located.

3. Run the following command:

   ```bash
   mvn test
   ```

   This command will compile your project and execute the tests. Maven will automatically discover and run all test classes that follow the naming conventions (e.g., *Test.java).

4. View the test results in the console. Maven will provide information about the tests that passed or failed.

### Note
If you want to run any maven commands without running the tests you can add the following flag
```
-DskipTests
```