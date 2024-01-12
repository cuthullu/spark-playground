package com.scottlogic.pod.spark.playground;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;

public class DataSchemas {

    public static List<StructField> peopleSchema = Arrays.asList(
            DataTypes.createStructField("Index", DataTypes.IntegerType, false));

    public static List<StructField> organisationSchema = Arrays.asList(
            DataTypes.createStructField("Index", DataTypes.IntegerType, false));

    public static List<StructField> peopleOrganisationSchema = Arrays.asList();
}
