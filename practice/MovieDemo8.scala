package spark_code_git

import java.util.regex.Pattern

import org.apache.spark.{SparkConf, SparkContext}

// 需求
// 分析每年度不同类型的电影总数 (year,类型),1
// 结果展示
// ((2000,Romance),17)
// ((2000,Horror),8)
// ((2000,Sci-Fi),10)
// ((2000,Action),19)
// ((2000,Mystery),1)
// ((2000,Documentary),8)
// ((2000,Children's),9)
// ((2000,Fantasy),1)
// ((2000,Comedy),69)
// ((2000,Musical),1)
// ((2000,Drama),55)
// ((2000,Crime),8)
// ((2000,War),2)
// ((2000,Thriller),25)
// ((2000,Animation),8)
// ((2000,Adventure),6)

object MovieDemo8 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local").setAppName("WordCount")
    val sc = new SparkContext(sparkConf)
    val filepath = "moive-data"
    val movies_rdd = sc.textFile(filepath + "/movies.dat")
    val pattern = Pattern.compile("(\\(\\d{4}\\))$")
    // "movies.dat"：MovieID::Title::Genres
    val resultRDD = movies_rdd
      .map(_.split("::"))
      .map(movie => {
        // 名称，类型
        (movie(1), movie(2))
      })
      .map(value => {
        var year = ""
        val matcher = pattern.matcher(value._1)
        if (matcher.find()) {
          year = matcher.group(1)
        }
        year = year.substring(1, 5)
        // 返回 （year，类型）
        (year, value._2)
      })
        // 类型不一定只有一个，例如  Comedy|Romance，需要进行扁平化处理
        .flatMapValues(_.split("\\|"))
        // （year，类型）,1
        .map(value => {
          ((value._1,value._2),1)
        })
//        .take(10).foreach(println)
      .reduceByKey(_ + _)
      // 查看2000年数据，检验数据逻辑
      .filter(_._1._1 == "2000")

    println("2000年各类型电影数量")
    resultRDD.collect().foreach(println)
    sc.stop()
  }

}
