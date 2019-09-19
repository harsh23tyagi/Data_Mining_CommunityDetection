import java.io.{File, FileWriter, Writer}


import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable.ArrayBuffer
import util.control.Breaks._


object harsh_tyagi_task1 {

  var outputfile = ""
  var inputfile = "" //"sample_data.csv"  // sys.argv[1]

  var adjacencyListMain = collection.mutable.Map[String, List[String]]()
  var threshold = 7.0
  var cost_dict = collection.mutable.Map[Tuple2[String, String], Double]()
  var totalEdges = 0
  var stricttotalNodesList = List[String]()
  var stricttotalNodesQueue = scala.collection.mutable.Queue[String]()
  var outputfile2 = ""

  def initialize(inputFile: String, outputFile1: String, outputFile2: String, threshold: Double): Unit ={

    val rootLogger = Logger.getRootLogger()
    rootLogger.setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[2]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val csvread = sc.textFile(inputFile)
    val columnName = csvread.first().toString().split(',').toList
    val items  = csvread.map(line => {line.split(',')}).filter(line => {(line(0) != columnName(0))}).map(line => (line(0), line(1)))

    var user_business_rdd = items.groupByKey().map(x=>(x._1, x._2.toSet))
    var user_business = user_business_rdd.collectAsMap()

    var length = user_business.size
    var users = user_business.keys.toList

//    println(user_business(users(0)))
    println("Creating Adjacency matrix.....")
    var tuple_edges = List[Tuple2[String, String]]()
    var c = 0
    for (i <- 0 to length-2){
      for(j <- (i+1) to length-1){
          var intersected = user_business(users(i)).intersect(user_business(users(j)))
          if(intersected.size>=7){
            tuple_edges = tuple_edges :+ Tuple2(users(i), users(j))
            tuple_edges = tuple_edges :+ Tuple2(users(j), users(i))
//            c = c+1
          }
      }
    }
    println("Created Adj. List")
    totalEdges = (tuple_edges.length)/2
    println(totalEdges)
    var adjacency_list = sc.parallelize(tuple_edges).groupByKey().mapValues(x=>x.toList).collectAsMap()

    adjacencyListMain = adjacency_list.asInstanceOf[collection.mutable.Map[String, List[String]]]
    stricttotalNodesList = adjacencyListMain.keys.toList

    for (each<-stricttotalNodesList){
      stricttotalNodesQueue = stricttotalNodesQueue :+ each
    }
    var totalNodes = stricttotalNodesQueue


  println("Finding betweenness...")

    var k3g = bfs(totalNodes, adjacency_list.asInstanceOf[collection.mutable.Map[String, List[String]]])
    writeToFile(k3g,outputfile)

    var allNodes = collection.mutable.Queue[String]()
    for (each<-stricttotalNodesList){
      allNodes = allNodes :+ each
    }

    println("--- :Creating Components: ---")
    createComponents(k3g, adjacencyListMain, allNodes, totalEdges)
    println("Finished")

  }

  //    ------------------------------------------Part 1 Begins------------------------------------------------
  def bfs(totalNodes: scala.collection.mutable.Queue[String], adjacencyList:collection.mutable.Map[String, List[String]] ): List[Any] ={

    cost_dict = collection.mutable.Map[Tuple2[String, String], Double]()

    while(totalNodes.length != 0){
      var root: String = totalNodes.dequeue()
//      println(root)
      NewBfsOnOne(root, adjacencyList)

    }
    for ((k,v) <- cost_dict){
      cost_dict(k) = v/2.0
    }
//    println(cost_dict)
//    println("Iterations: "+cost_dict.size)

    var k3g: List[Any] = sorting(cost_dict)
//    println(k3g)


    return k3g
  }

  //The function below is girvan neuman
  def NewBfsOnOne(rt: String, adjList: collection.mutable.Map[String, List[String]]): Unit ={
    var root = rt
    var queue = scala.collection.mutable.Queue[String]()

    var parent = collection.mutable.Map[String, List[String]]()
    parent(root)= List()
    queue = queue:+root
    var visited = collection.mutable.Map[String,Int]()
    visited(root) = 1
    var level_nodes = collection.mutable.Map[Int, List[String]]()
    var level = collection.mutable.Map[String, Int]()
    var maximum = 0
    var label = collection.mutable.Map[String, Double]()
    label(root) = 1.0
    level(root) = 0
    while(!queue.isEmpty){

      var current = queue.dequeue()

      if(!adjList.contains(current)){}
      else{
        //everything goes in this else
        for(each<-adjList(current)){

          if(visited.contains((each))){
            if(level(each)==(level(current)+1)){
              //println("Amount of times....")
              parent(each) = parent(each) :+ current
              label(each) += label(current) //check label
            }
          }

          //if not in visited
          else{
            if(parent.contains(each)){
              parent(each) = parent(each) :+ current
            }
            else{
              parent(each) = List(current)
            }
            label(each) = label(current)
            level(each) = level(current)+1
            visited(each)= 1
//            println("")
//            println(visited)
//            println("")
            queue= queue :+ each

            if(level_nodes.contains(level(current)+1)){
              level_nodes(level(current)+1) = level_nodes(level(current)+1) :+ each
            }
            else{
              level_nodes(level(current)+1) = List(each)
            }
            if((level(current)+1) > maximum){
              maximum = (level(current)+1)
            }


          }
        }
      //outside else
      }



    }
//    println(root)
//    println(label)
//    println(parent)
//    println(level)
//    println(level_nodes)
//    println(maximum)
//    println("-------------")

    var k = maximum
    var cost_node = collection.mutable.Map[String, Double]()
    var flag = true
    while(k>0){

      var current_list = level_nodes(k)

      for(each<- current_list){
        var parent_list = parent(each)
        var cost_each = 1.0
        var flag = true
//        Step 1
        if(cost_node.contains(each)){
          cost_each = cost_node(each)
        }
        else{
          cost_node(each) = 1.0
        }


//        Step 2

        var total = 0.0
        for (par <- parent_list){
          total = total + label(par)
        }

        if(total == 0){
          var temp = List(root, each).sorted
          var tup = Tuple2[String, String](temp(0),temp(1))

          if(cost_dict.contains(tup)){
            cost_dict(tup) = cost_dict(tup) + cost_each
          }
          else{
            cost_dict(tup) = cost_each
          }

          //here continue
          flag = false
        }


//        Step 3
        if(flag){
          for (par <- parent_list){
            var div: Double = label(par)/total
            if(cost_node.contains(par)){
              cost_node(par) =  cost_node(par)+ (cost_each*div)
            }
            else{
              cost_node(par) = 1.0+(cost_each*div)
            }
            var temp = List(par, each).sorted
            var tup = Tuple2[String, String](temp(0),temp(1))
            if(cost_dict.contains(tup)){
              cost_dict(tup) = cost_dict(tup) + (cost_each*div)
            }
            else{
              cost_dict(tup) = (cost_each*div)
            }
          }
        }

      }
    k = k-1
    }
//    println(cost_node)
//    println(root)
//    println(cost_dict)
//    println("===============-")
//    println("")
  }


  def writeToFile(listFinal: List[Any], outputfile: String): Unit ={
    val f = new File(outputfile);
    f.createNewFile();
    val w: Writer = new FileWriter(f)

    for(each<-listFinal){

      var check = each.asInstanceOf[Tuple2[Tuple2[String,String],Double]]
      w.write("('"+check._1._1+"', '"+check._1._2+"'), "+check._2+"\n")

    }
    w.close()

    println("completed")
  }


  def createComponents(list_val2:List[Any], mainAdj: collection.mutable.Map[String, List[String]], totalNodes: scala.collection.mutable.Queue[String], totalEdges: Int): Unit ={
    var mod_0 = 0.0
    var count = 0
    var newModularity = mod_0
    var allNodes = totalNodes
    var mainAd = mainAdj
    var maxComp = collection.mutable.Map[Int, List[String]]()
    var list_val = list_val2
    var max =0.0
    while(!list_val.isEmpty){ //
//      allNodes should be a queue

      var comp = endingBFS(mainAdj, allNodes)

      if(count==0){
        mod_0 = calculateModularity(mainAdj,comp,totalNodes, totalEdges)

      }

      count +=1
//      println("FirstModularity: "+mod_0)
      var node1:String = list_val.asInstanceOf[List[Tuple2[Tuple2[String, String], Double]]](0)._1._1
      var node2: String = list_val.asInstanceOf[List[Tuple2[Tuple2[String, String], Double]]](0)._1._2

      mainAdj(node1) = (mainAdj(node1).toSet - node2).toList
      mainAdj(node2) = (mainAdj(node2).toSet - node1).toList

      newModularity = calculateModularity(
        mainAdj, comp, totalNodes, totalEdges)

      if(mod_0<newModularity){
        mod_0 = newModularity
        maxComp = comp
      }
//      println("NewModularity: "+newModularity)
      for (each<-stricttotalNodesList){
        allNodes = allNodes :+ each
      }
      bfs(allNodes,mainAdj)
      list_val = List[Any]()
      list_val = sorting(cost_dict)

      //println(list_val)
      for (each<-stricttotalNodesList){
        allNodes = allNodes :+ each
      }


//      println("Size Current: "+comp.size)
//      println(newModularity)
//    println("Iteration: "+count)

    }

    println("--Communities---")
    println((maxComp.size))
    println("Iterations-: "+count)
    println("Total Communities: "+maxComp.size)
    println("Writing to file....")
    writingCommunities(maxComp)
  }






  def endingBFS(adjMatrix: collection.mutable.Map[String, List[String]], allNodes: scala.collection.mutable.Queue[String]): collection.mutable.Map[Int, List[String]] ={
    var unvisitedNodes = allNodes

    var visitedNodes = scala.collection.mutable.Queue[String]()
    var components = collection.mutable.Map[Int, List[String]]()
    var i = 0
    var count = 1
    while(!unvisitedNodes.isEmpty){
      var root = unvisitedNodes.dequeue()
      var queue = scala.collection.mutable.Queue[String]()
      queue = queue :+ root
      visitedNodes = scala.collection.mutable.Queue[String]()
      visitedNodes = visitedNodes :+ root
      while(!queue.isEmpty){
        var rt = queue.dequeue()
        var adj_nodes = adjMatrix(rt)
        for(each<-adj_nodes){
          if(!visitedNodes.contains(each)){
            queue = queue :+ each
            visitedNodes = visitedNodes :+ each
          }
        }
      }

//      convert list into sets
      var inter = unvisitedNodes.toSet
      for (each<-visitedNodes){
          inter = inter - each
      }
      unvisitedNodes = scala.collection.mutable.Queue[String]()
      for (each<- inter){
        unvisitedNodes = unvisitedNodes :+ each
      }

      components(i) = visitedNodes.toList
      i +=1
      count +=1
    }
  return components
  }




  def calculateModularity(component_adj_list: collection.mutable.Map[String, List[String]], communities: collection.mutable.Map[Int, List[String]], totalNodesP: scala.collection.mutable.Queue[String], totalEdges: Int): Double ={
    //println(totalEdges)
    var mul = 1.0/(2.0* totalEdges.toDouble)
    var total = 0.0
    var totalNodes = totalNodesP.toList
    var modularityComponent = 0.0


    for ((community,nodes)<-communities){
      total = 0.0
      var length = nodes.length
      for (i<- 0 to length-2){
        var adj_nodes = adjacencyListMain(nodes(i))
        var a_ij = 0.0
        var k_i = adj_nodes.length.toDouble

        for(j<- (i+1) to length-1){
          if(adj_nodes.contains(nodes(j))){
            a_ij = 1.0
          }
          else{
            a_ij = 0.0
          }

          var k_j = adjacencyListMain(nodes(j)).length.toDouble
          total += a_ij - ((k_i*k_j)/(2.0*totalEdges.toDouble))

        }
      }
      modularityComponent += total
    }


    return modularityComponent*mul

  }






  def check(): Unit ={
    var adj_list = collection.mutable.Map[String, List[String]]()

//    adj_list("1") = List("2", "3")
//    adj_list("2") = List("1", "3")
//    adj_list("3") = List("1", "2", "7")
//    adj_list("4") = List("5", "6")
//    adj_list("5") = List("4", "6")
//    adj_list("6") = List("4", "5", "7")
//    adj_list("7") = List("3", "6", "8")
//    adj_list("8") = List("9", "12", "7")
//    adj_list("9") = List("8", "10", "11")
//    adj_list("10") = List("9", "11")
//    adj_list("11") = List("9", "10")
//    adj_list("12") = List("8", "13", "14")
//    adj_list("13") = List("12", "14")
//    adj_list("14") = List("12", "13")
//
//
//
//    totalEdges = 17
//    println(totalEdges)
//    Another Adjacency Matrix
//    adj_list("A") = List("B", "C")
//    adj_list("B") = List("A", "C", "K")
//    adj_list("C") = List("A", "B", "F", "K")
//    adj_list("D") = List("E", "F")
//    adj_list("E") = List("D", "F")
//    adj_list("F") = List("C", "D", "E")
//    adj_list("K") = List("B", "C")
//
//    totalEdges = 9

//    adj_list("A") = List("B", "C")
//    adj_list("B") = List("A", "C", "D")
//    adj_list("C") = List("A", "B")
//    adj_list("D") = List("B", "E", "F", "G")
//    adj_list("E") = List("D", "F")
//    adj_list("F") = List("E", "D", "G")
//    adj_list("G") = List("D", "F")
//
//    totalEdges = 9

    adj_list("A") = List("B", "E")
    adj_list("B") = List("A", "F", "G")
    adj_list("G") = List("B", "D")
    adj_list("D") = List("G", "F")
    adj_list("E") = List("A", "F")
    adj_list("F") = List("B", "D", "E")

    totalEdges = 7

    adjacencyListMain = adj_list
//    println(adjacencyListMain)
//    stricttotalNodes = adjacencyListMain.keys.toList
//    var totalNodes = stricttotalNodes
//    println(totalNodes)
    stricttotalNodesList = adjacencyListMain.keys.toList

    for (each<-stricttotalNodesList){
      stricttotalNodesQueue = stricttotalNodesQueue :+ each
    }
    var totalNodes = stricttotalNodesQueue
    cost_dict = collection.mutable.Map[Tuple2[String, String], Double]()
    var count = 1
    while(totalNodes.length != 0){
      var root: String = totalNodes.dequeue()
//      println(root)
      NewBfsOnOne(root, adj_list)
//      count += 1
    }



    for ((k,v) <- cost_dict){
      cost_dict(k) = v/2.0
    }
    println(cost_dict)
//    println("Iterations: "+cost_dict.size)

    var k3g = sorting(cost_dict)
    println(k3g)
    writeToFile(k3g, outputfile)

    for (each<-stricttotalNodesList){
      totalNodes = totalNodes :+ each
    }
//    println("BREAKING POINT::"+totalNodes.length)
    createComponents(k3g, adjacencyListMain, totalNodes, totalEdges)
  }





  def sorting(finalMap: collection.mutable.Map[Tuple2[String,String], Double]): List[Any]={
    var kl = finalMap.toSeq.sortWith((a, b) => a._2 > b._2 || (a._2 == b._2 && (a._1.toString()<b._1.toString())))
//    println(kl.toList)
    return kl.toList
  }


  def writingCommunities(finalMap: collection.mutable.Map[Int, List[String]]): Unit ={
    var mainList = List[List[String]]()

    for((k,v)<-finalMap)
    {
      mainList=mainList :+ v.sorted
    }

    //var finalList = (mainList.sortBy(x=> x.length))
    var finalList = mainList.sortWith((a,b)=> a.length<b.length ||(a.length == b.length && a.toString() < b.toString()))
    val f = new File(outputfile2);
    f.createNewFile();
    val w: Writer = new FileWriter(f)


    var i = 0
    for (each<-finalList){
      w.write("'"+each(0)+"'")
      i = 0
      for(sub<-each){
        if(i!=0){
          w.write(", '"+sub+"'")
        }

        i+=1

      }
      w.write("\n")
    }
    w.close()

  }


  def main(args: Array[String]): Unit = {
    var start_time = System.currentTimeMillis()


    threshold = args(0).toDouble
    inputfile = args(1)
    outputfile = args(2)
    outputfile2 = args(3)
    println(inputfile)
    initialize(inputfile, outputfile, outputfile2, threshold)

    var end_time = System.currentTimeMillis()
    var Duration = (end_time-start_time)/1000
    println("\nDuration: "+Duration)

  }
}



