import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

trait DataFrameTestUtils {

  def assertSchema(schema1: StructType, schema2: StructType, checkNullable: Boolean = true): Boolean = {
    val s1 = schema1.fields.map(f => (f.name, f.dataType, f.nullable))
    val s2 = schema2.fields.map(f => (f.name, f.dataType, f.nullable))
    if (checkNullable) {
      s1.diff(s2).isEmpty
    }
    else {
      s1.map(s => (s._1, s._2)).diff(s2.map(s => (s._1, s._2))).isEmpty
    }
  }

  def assertData(df1: DataFrame, df2: DataFrame): Boolean = {
    val data1 = df1.collect()
    val data2 = df2.collect()
    data1.diff(data2).isEmpty
  }
}


import spark.implicits._

val sourceDf = Seq(
  ("n0001", "1", "harrry potter"),
  ("n0002", "2", "titanic"),
  ("n0003", "3", "sherlockHomes")
).toDF("titleId", "ordering", "title")



val expectedDf = Seq(
("n0001", "1", "harrry potter","uk-east-1","french"),
  ("n0002", "2", "titanic","uk-west-2","english"),
  ("n0003", "3", "sherlockHomes","uk_east-2","spanish")
).toDF("titleId", "ordering", "title","region","language")


test("DataFrame Schema Test") {


assert(assertSchema(sourceDf.schema, expectedDf.schema))

 assert(assertData(sourceDf, expectedDf))
}


### to mock up source DF , you can create a dummy csv file to read data from source
df=spark.read.option("header","true")\
    .option("delimiter"," ")\
    .option("inferSchema","true")\
    .schema(
        StructType(
            [
                StructField("titleId",StringType()),
                StructField("ordering",IntegerType(),
				StructField("title",StringType)
            ]
        )
    )\
    .csv("file:/home/maria_dev/imdb.csv") 
