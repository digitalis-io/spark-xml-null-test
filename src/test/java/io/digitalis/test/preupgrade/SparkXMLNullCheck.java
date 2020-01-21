package io.digitalis.test.preupgrade;


import com.databricks.spark.xml.XmlReader;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Test;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.assertEquals;

public class SparkXMLNullCheck {


    private static final SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("xmltest");
    private static final SparkContext sc = SparkContext.getOrCreate(conf);
    private static final SQLContext sqlContext = new SQLContext(sc);
    private static final JavaSparkContext jsc = JavaSparkContext.fromSparkContext(sc);

    @Test
    public void checkEmptyXMLNodesAreNullInDatasetWhenAttributeLeftUnset() throws Exception {

        DataFrame dataframe = new XmlReader()
                .withSchema(getSchema())
                .withRowTag("profiles")
                .xmlFile(sqlContext,"src/test/resources/payload.xml");

        assertEquals(1, dataframe.select("profile.address.line2").where(col("line2").isNull()).count());

    }

    @Test
    public void checkEmptyXMLNodesAreNullInDatasetWhenAttributeSetToTrue() throws Exception {

        DataFrame dataframe = new XmlReader()
                .withSchema(getSchema())
                .withRowTag("profiles")
                .withTreatEmptyValuesAsNulls(true)
                .xmlFile(sqlContext,"src/test/resources/payload.xml");


        assertEquals(1, dataframe.select("profile.address.line2").where(col("line2").isNull()).count());

    }

    @Test
    public void checkEmptyXMLNodesAreNullInDatasetEvenWhenAttributeSetToFalse() throws Exception {

        DataFrame dataframe = new XmlReader()
                .withSchema(getSchema())
                .withRowTag("profiles")
                .withTreatEmptyValuesAsNulls(false)
                .xmlFile(sqlContext,"src/test/resources/payload.xml");

        assertEquals(1, dataframe.select("profile.address.line2").where(col("line2").isNull()).count());

    }


    @AfterClass
    public static void tearDown() {
        jsc.close();
    }


    private StructType getSchema() {

        StructType addressType = new StructType()
                .add("city", DataTypes.StringType)
                .add("country", DataTypes.StringType)
                .add("county", DataTypes.StringType)
                .add("line1", DataTypes.StringType)
                .add("line2", DataTypes.StringType)
                .add("line3", DataTypes.StringType)
                .add("postalcode", DataTypes.StringType);


        StructType profileType = new StructType()
                .add("address", addressType)
                .add("firstname", DataTypes.StringType)
                .add("surname", DataTypes.StringType);

        return new StructType().add("profile", profileType);

    }


}
