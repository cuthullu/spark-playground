Feature: Count Words

  Scenario: Words are counted correctly
    Given a new wordCount
    When I supply the path "data/example-words.txt"
    Then I should get a count of 188

  Scenario: Words are counted incorrectly
    Given a new wordCount
    When I supply the path "data/example-words-fewer-words.txt"
    Then I should not get a count of 188

  Scenario: An exception is thrown for null input
    Given a new wordCount
    When I supply the path "data/example-words-null.txt"
    Then I should get an exception
