package com.scottlogic.pod.spark.playground;

import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class PeopleAndOrganisations {
    public static void main(String[] args) {

        SparkSession spark = SparkSession.builder()
                .appName("SparkPlayground")
                .master("local[*]")
                // .master("spark://localhost:7077") // Switch to this to run against a local
                // cluster
                .enableHiveSupport()
                .getOrCreate();

        SparkContext cs = spark.sparkContext();
        JavaSparkContext jcs = new JavaSparkContext(cs);

        /*
         * There are three datasets in the data directory
         * - a people file with a list of people
         * - an organisation file with a list of organisations
         * - a peoples-organisations file linking people to organisations
         * 
         * I have split the tasks into sections. It is advised to do them in order.
         * The results of each section can, but don't necessarily, feed into the next
         * i.e. you might not need to use all the tables, results again
         */

        /*
         * Section: importing
         */

        /*
         * Task: import and read each of the CSVs, giving them proper schemas
         * - data/people-100000.csv
         * - data/peoples-organisations.csv
         * - data/organisations-100.csv
         */

        Dataset<Row> peopleDF = spark.read()
                .option("header", "true")
                .schema(DataTypes.createStructType(DataSchemas.peopleSchema))
                .csv("./data/people-100000.csv")
                .toDF();

        Dataset<Row> peopleOrgsDF;

        Dataset<Row> orgsDF;

        /*
         * Section: basic joining
         */

        /*
         * Task: Now join the people with their organisations using the linking table
         */

        Dataset<Row> combinedDf;
        ;

        /*
         * Section: Email fixing
         * All user's emails are currently using example domains.
         */

        /*
         * Task: find all the different example email domains being used
         */

        /*
         * Task: Now fix user's email addresses by changing the domain of their email
         * address to the company's domain extracted from their website.
         * e.g. if a user's email was clairebradshaw@example.org
         * and their organisation's website was http://www.hall-buchanan.info/
         * the email should be changed to clairebradshaw@hall-buchanan.info
         */

        /*
         * Task: Show a list of all the email address
         */

        /*
         * Section: People counting
         * Each organisation row has a "Number of employees" listed.
         * We want to find out if that number matches the number of people we have
         * listed for them
         */

        /*
         * Task: count the number of people belonging to each organisation
         */

        /*
         * Task: combine that count with list of organisations
         */

        /*
         * Task: Add a column "Employee count status" that can have a value of "More",
         * "Less" or "Equal"
         * Where it is:
         * - "Less" if we have less people than the stated Number of employees
         * - "More" if we have more people than the stated Number of employees
         * - "Equal" if we have same amount of people as the stated Number of employees
         */

        /*
         * Task: find out how many companies have each status
         */

        /*
         * Section: Employability
         * We have Job Titles listed for each person
         * Lets find out some info on which jobs are most in demand
         */

        /*
         * Task: Find out how many are people are employed for each job, list the top 10
         */

        /*
         * Task: Find out how many different companies have people employed for the same
         * job, list the top 10
         */

        /*
         * Task: Find out which jobs are used across the most industries, list the top
         * 10
         * 
         */

        /*
         * Task: Find out which industry has the most diverse set of job involved
         */

        spark.stop();

    }
}
