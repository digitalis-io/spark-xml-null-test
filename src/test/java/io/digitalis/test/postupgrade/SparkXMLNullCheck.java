package io.digitalis.test.postupgrade;


import com.databricks.spark.xml.XmlReader;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.AfterClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.junit.Assert.assertEquals;

public class SparkXMLNullCheck {

    private static final SparkSession session = SparkSession.builder().master("local[2]").getOrCreate();
    private static final JavaSparkContext jsc = JavaSparkContext.fromSparkContext(session.sparkContext());

    @Test
    public void checkEmptyXMLNodesAreNotNullInDataset() throws Exception {


        Dataset<Row> dataset = new XmlReader()
                                    .withSchema(getSchema())
                                    .withRowTag("profiles")
                                    .xmlFile(session,"src/test/resources/payload.xml");


        assertEquals(1, dataset.select("profile.address.line2").where(col("line2").isNotNull()).count());


    }


    @Test
    public void checkEmptyXMLNodesAreNullInDataset() throws Exception {


        Dataset<Row> dataset = new XmlReader()
                .withSchema(getSchema())
                .withRowTag("profiles")
                .withTreatEmptyValuesAsNulls(true)
                .xmlFile(session,"src/test/resources/payload.xml");

        assertEquals(1, dataset.select("profile.address.line2").where(col("line2").isNull()).count());


    }

    @AfterClass
    public static void tearDown() {
        jsc.close();
        session.close();
    }


    private StructType getSchema() {

        List<StructField> addressFields = new ArrayList<>();
        addressFields.add(DataTypes.createStructField("line1",      DataTypes.StringType,true));
        addressFields.add(DataTypes.createStructField("line2",      DataTypes.StringType,false));
        addressFields.add(DataTypes.createStructField("line3",      DataTypes.StringType,true));
        addressFields.add(DataTypes.createStructField("city",       DataTypes.StringType,true));
        addressFields.add(DataTypes.createStructField("county",     DataTypes.StringType,true));
        addressFields.add(DataTypes.createStructField("postalcode", DataTypes.StringType,true));
        addressFields.add(DataTypes.createStructField("country",    DataTypes.StringType,true));

        List<StructField> profile = new ArrayList<>();
        profile.add(DataTypes.createStructField("firstname", DataTypes.StringType, true));
        profile.add(DataTypes.createStructField("surname", DataTypes.StringType, true));
        profile.add(DataTypes.createStructField("address", DataTypes.createStructType(addressFields), true));

        StructField root = DataTypes.createStructField("profile",DataTypes.createStructType(profile), true);

        StructType schema = DataTypes.createStructType(new StructField[]{root});

        return schema;

    }

}
