## Instructions

This is a Maven project with two different profiles that allow testing of two different versions 
of Spark/DSE and Spark-XML to show that they treat empty XML elements differently.

To execute with Spark 1.6 and Spark-XML 0.3.3 execute the following command:
```
mvn -Psparkxml_3_3_0 clean test -Dtest=io.digitalis.test.preupgrade.SparkXMLNullCheck
```
The results show that Spark XML treats empty elements as nulls *_even if_* the attribute `withTreatEmptyValuesAsNulls` is set to false.

To execute with Spark 2.2.3 and Spark-XML 0.5.0 execute the following command:
```
mvn -Psparkxml_5_0_0 clean test -Dtest=io.digitalis.test.postupgrade.SparkXMLNullCheck
```
This illustrates that the treatment of empty elements can be controlled correctly using `withTreatEmptyValuesAsNulls`


