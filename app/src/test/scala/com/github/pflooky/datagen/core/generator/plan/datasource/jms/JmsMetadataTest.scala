package com.github.pflooky.datagen.core.generator.plan.datasource.jms

import com.github.pflooky.datagen.core.generator.Holder
import com.github.pflooky.datagen.core.util.{ProtobufUtil, SparkSuite}
import org.apache.spark.sql.avro.functions.from_avro
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.protobuf.functions.from_protobuf
import org.apache.spark.sql.{Encoder, Encoders}

import java.io.File
import scala.collection.JavaConverters.mapAsJavaMapConverter

class JmsMetadataTest extends SparkSuite {

  test("can get avro data with schema") {
    implicit val encoder: Encoder[String] = Encoders.kryo[String]
    val avroSchema =
      """{"namespace": "example.avro",
        | "type": "record",
        | "name": "User",
        | "fields": [
        |     {"name": "name", "type": "string", "default": "peter"},
        |     {"name": "age", "type": "int"},
        |     {"name": "favorite_color", "type": ["string", "null"]},
        |     {"name": "favorite_numbers", "type": {"items": "int", "type": "array"}}
        | ]
        |}""".stripMargin
    //    val protobufData = sparkSession.emptyDataset[ColumnMetadata]
    //      .selectExpr("from_protobuf('', ') AS event")
    //    protobufData.printSchema()
    val avroData = sparkSession.createDataFrame(Seq.fill(10)(1).map(Holder))
      .select(from_avro(lit("".getBytes), avroSchema) as "event")
    //has the correct schema but doesn't contain any additional metadata (i.e. no default)
    avroData.printSchema()
  }


  /**
   * Create descriptor file via command like this:
   * protoc --include_imports --descriptor_set_out=<output_file>.desc --proto_path <proto_folder> <proto_file>.proto
   */
  test("can read protobuf") {
    val protoFile = new File("app/src/test/resources/sample/files/protobuf/example.desc").getAbsolutePath
    val protobufData = sparkSession.createDataFrame(Seq.fill(10)(1).map(Holder))
      .select(from_protobuf(lit("".getBytes), "Proto3AllTypes", protoFile))
    protobufData.printSchema()
  }

  test("can read all structs from proto descriptor file") {
    val protoFile = new File("app/src/test/resources/sample/files/protobuf/example.desc").getAbsolutePath
    val structs = ProtobufUtil.toStructType(protoFile)
    structs
  }

}
