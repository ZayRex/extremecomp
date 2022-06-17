package imdb

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]]) {
  def getGenres(): List[String] = genres.getOrElse(List[String]())
}
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {

  val conf: SparkConf = new SparkConf().setAppName("Imdb Analysis").setMaster("local")
  val sc: SparkContext = new SparkContext(conf)

  // Hint: use a combination of `ImdbData.titleBasicsPath` and `ImdbData.parseTitleBasics`
  val titleBasicsRDD: RDD[TitleBasics] = sc.textFile(ImdbData.titleBasicsPath).map(ImdbData.parseTitleBasics)

  // Hint: use a combination of `ImdbData.titleRatingsPath` and `ImdbData.parseTitleRatings`
  val titleRatingsRDD: RDD[TitleRatings] = sc.textFile(ImdbData.titleRatingsPath).map(ImdbData.parseTitleRatings)

  // Hint: use a combination of `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew`
  val titleCrewRDD: RDD[TitleCrew] = sc.textFile(ImdbData.titleCrewPath).map(ImdbData.parseTitleCrew)

  // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  val nameBasicsRDD: RDD[NameBasics] = sc.textFile(ImdbData.nameBasicsPath).map(ImdbData.parseNameBasics)


  def task1(rdd: RDD[TitleBasics]): RDD[(Float, Int, Int, String)] = {
    rdd.filter(x => x.genres.isDefined && x.runtimeMinutes.isDefined) //filter the Nones out
      .flatMap(x => x.genres.get.map((x.runtimeMinutes.get, _))) // create an rdd of tuples between (runtime, genre)
      .groupBy(_._2).map{case (genre,runtime) => (genre, runtime.map(_._1))} //group by genre
      .map(x => (x._2.sum.toFloat/x._2.size.toFloat, x._2.min, x._2.max, x._1))
  }

  def task2(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[String] = {
    val l1_pair = l1.filter(x => x.titleType.getOrElse("")=="movie" && x.startYear.getOrElse(0)>=1990 && x.startYear.getOrElse(3000)<=2018).map(x => x.tconst -> x.primaryTitle)
    val l2_pair = l2.filter(x => x.numVotes>=500000 && x.averageRating>=7.5).map(x=>(x.tconst,x))
    l1_pair.join(l2_pair).map(x => x._2._1.get)
  }

  def task3(l1: RDD[TitleBasics], l2: RDD[TitleRatings]): RDD[(Int, String, String)] = {
    val avgRate_pair = l2.map(x=> x.tconst -> x.averageRating).collect().toMap

    val filteredL1 = l1.filter(x => x.genres.isDefined && x.titleType.getOrElse("") == "movie" && x.startYear.getOrElse(0) >= 1900 && x.startYear.getOrElse(3000) <= 1999 && avgRate_pair.contains(x.tconst)) //filter the type and range of dates
    val groupedByG = filteredL1.flatMap(x => x.genres.get.map((_, x.primaryTitle.get, x.startYear.get, avgRate_pair.get(x.tconst)))).groupBy(_._1)
    val groupByD = groupedByG.map { case (k, v) => (k, v.groupBy(x =>(x._3 - 1900)/10)) }.flatMap { case (genre, groupByDecade) => (groupByDecade.map { case (decade, v) => (decade, genre, v.toList.sortBy(_._2).maxBy(_._4)._2)})}.sortBy(x => x)

    groupByD
  }

  // Hint: There could be an input RDD that you do not really need in your implementation.
  def task4(l1: RDD[TitleBasics], l2: RDD[TitleCrew], l3: RDD[NameBasics]): RDD[(String, Int)] = {
    val filtered_l1 = l1.filter(x => x.startYear.getOrElse(0) >= 2010 && x.startYear.getOrElse(3000) <= 2021).map(_.tconst).collect().toSet //filter tconsts between 2010 and 2021
    l3.filter(x => x.primaryName.isDefined && x.knownForTitles.isDefined)
      .map( x => (x.primaryName.get, x.knownForTitles.get.toSet.intersect(filtered_l1).size))
      .filter(_._2 >= 2)
  }
  def task4(l1: RDD[TitleBasics], l2: RDD[TitleCrew], l3: RDD[NameBasics]): RDD[(String, Int)] = {

    // list of distinct title IDs in range
    val filmsInRange = l1.filter(t=>t.startYear.isDefined && t.startYear.get>=2010 && t.startYear.get<=2021)
      .map(m=>(m.tconst,1))//(tconst, 1)

    val names = l3.filter(n=> n.primaryName.isDefined && n.knownForTitles.isDefined)
      .flatMap(t=>t.knownForTitles.get.map(x=>(x, (t.nconst, t.primaryName.get))))

    val output = names.map(n=>(n._2._1,n._2._2))
      .join(names.join(filmsInRange).map(x=>(x._2._1._1,x._2._2))
        .reduceByKey(+)).distinct().map(x=>x._2).filter(x=>x._2>=2)
    return output
  }

  def main(args: Array[String]) {
    val durations = timed("Task 1", task1(titleBasicsRDD).collect().toList)
    val titles = timed("Task 2", task2(titleBasicsRDD, titleRatingsRDD).collect().toList)
    val topRated = timed("Task 3", task3(titleBasicsRDD, titleRatingsRDD).collect().toList)
    val crews = timed("Task 4", task4(titleBasicsRDD, titleCrewRDD, nameBasicsRDD).collect().toList)
    println(durations)
    println(titles)
    println(topRated)
    println(crews)
    println(timing)
    sc.stop()
  }

  val timing = new StringBuffer
  def timed[T](label: String, code: => T): T = {
    val start = System.currentTimeMillis()
    val result = code
    val stop = System.currentTimeMillis()
    timing.append(s"Processing $label took ${stop - start} ms.\n")
    result
  }
}
