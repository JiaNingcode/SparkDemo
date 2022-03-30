package Spark01

import org.apache.spark.sql.SparkSession

object Demo2 {
  def main(args: Array[String]): Unit = {
    val sparksql=SparkSession.builder().enableHiveSupport().getOrCreate()

    sparksql.sql(
      """
        |create database if not exists ods_user;
        |use ods_user;
        |create table if not exists
        |""".stripMargin)
  }
}
