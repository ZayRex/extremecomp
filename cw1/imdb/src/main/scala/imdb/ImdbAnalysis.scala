package imdb

case class TitleBasics(tconst: String, titleType: Option[String], primaryTitle: Option[String],
                      originalTitle: Option[String], isAdult: Int, startYear: Option[Int], endYear: Option[Int],
                      runtimeMinutes: Option[Int], genres: Option[List[String]])
case class TitleRatings(tconst: String, averageRating: Float, numVotes: Int)
case class TitleCrew(tconst: String, directors: Option[List[String]], writers: Option[List[String]])
case class NameBasics(nconst: String, primaryName: Option[String], birthYear: Option[Int], deathYear: Option[Int],
                      primaryProfession: Option[List[String]], knownForTitles: Option[List[String]])

object ImdbAnalysis {

  // Hint: use a combination of `ImdbData.titleBasicsPath` and `ImdbData.parseTitleBasics`
  val titleBasicsList: List[TitleBasics] = readFile(ImdbData.titleBasicsPath).map(ImdbData.parseTitleBasics)

  // Hint: use a combination of `ImdbData.titleRatingsPath` and `ImdbData.parseTitleRatings`
  val titleRatingsList: List[TitleRatings] = readFile(ImdbData.titleRatingsPath).map(ImdbData.parseTitleRatings)

  // Hint: use a combination of `ImdbData.titleCrewPath` and `ImdbData.parseTitleCrew`
  val titleCrewList: List[TitleCrew] = readFile(ImdbData.titleCrewPath).map(ImdbData.parseTitleCrew)

  // Hint: use a combination of `ImdbData.nameBasicsPath` and `ImdbData.parseNameBasics`
  val nameBasicsList: List[NameBasics] = readFile(ImdbData.nameBasicsPath).map(ImdbData.parseNameBasics)

  def readFile(path: String): List[String] ={
    val f = scala.io.Source.fromFile(path)
    val lines = f.getLines().toList
    f.close()
    lines
  }
  def task1(list: List[TitleBasics]): List[(Float, Int, Int, String)] = {

    list.filter(x => x.genres.isDefined && x.runtimeMinutes.isDefined) //filter the Nones out
      .flatMap(x => x.genres.get.map((x.runtimeMinutes.get, _))) // create a list of tuples between (runtime, genre)
      .groupBy(_._2).map{case (genre,runtime) => (genre, runtime.map(_._1))} //group by genre and produce Map(genre,List[runtime])
      .map(x => (x._2.sum.toFloat/x._2.size.toFloat, x._2.min, x._2.max, x._1)).toList // calculate avg, min and max
  }

  def task2(l1: List[TitleBasics], l2: List[TitleRatings]): List[String] = {
    val l1_map = l1.filter(x => x.titleType.getOrElse("")=="movie" && x.startYear.getOrElse(0)>=1990 && x.startYear.getOrElse(3000)<=2018).map(x => x.tconst -> x.primaryTitle).toMap
    val l2_set = l2.filter(x => x.numVotes>=500000 && x.averageRating>=7.5).map(_.tconst).toSet
    l1_map.filterKeys(l2_set.contains).values.flatten.toList
  }

  def task3(l1: List[TitleBasics], l2: List[TitleRatings]): List[(Int, String, String)] = { //List[(Int, String, String, Float, Int)] v.maxBy(_._4)._4.get, v.size

    val avgRateMap = l2.map(x=> x.tconst -> x.averageRating).toMap

    val filteredL1 = l1.filter(x => x.genres.isDefined && x.titleType.getOrElse("") == "movie" && x.startYear.getOrElse(0) >= 1900 && x.startYear.getOrElse(3000) <= 1999 && avgRateMap.contains(x.tconst)) //filter the type and range of dates
    val groupedByG = filteredL1.flatMap(x => x.genres.get.map((_, x.primaryTitle.get, x.startYear.get, avgRateMap.get(x.tconst)))).groupBy(_._1)
    val groupByD = groupedByG.map { case (k, v) => (k, v.groupBy(x =>(x._3 - 1900)/10)) }.flatMap { case (genre, groupByDecade) => (groupByDecade.map { case (decade, v) => (decade, genre, v.sortBy(_._2).maxBy(_._4)._2)  }) }.toList.sorted

    groupByD
  }

  // Hint: There could be an input list that you do not really need in your implementation.
  def task4(l1: List[TitleBasics], l2: List[TitleCrew], l3: List[NameBasics]): List[(String, Int)] = {

    val filtered_tconsts = l1.filter(x => x.startYear.getOrElse(0) >= 2010 && x.startYear.getOrElse(3000) <= 2021) //filter tconsts between 2010 and 2021
      .map(_.tconst).toSet //produce a set of tconsts
    l3.filter(x => x.primaryName.isDefined && x.knownForTitles.isDefined).map( x => (x.primaryName.get, x.knownForTitles.get.toSet.intersect(filtered_tconsts).size)).filter(_._2 >= 2)
  }

  def main(args: Array[String]) {
    val durations = timed("Task 1", task1(titleBasicsList))
    val titles = timed("Task 2", task2(titleBasicsList, titleRatingsList))
    val topRated = timed("Task 3", task3(titleBasicsList, titleRatingsList))
    val crews = timed("Task 4", task4(titleBasicsList, titleCrewList, nameBasicsList))
    println(durations)
    println(titles)
    println(topRated)
    println(crews)
    println(timing)
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
