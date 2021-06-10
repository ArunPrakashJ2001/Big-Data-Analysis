val data_14 = sc.textFile("Chicago_Crimes_2001_to_2004_pre.csv")
val data_57 = sc.textFile("Chicago_Crimes_2005_to_2007_pre.csv")
val data_811 = sc.textFile("Chicago_Crimes_2008_to_2011_pre.csv")
val data_1217 = sc.textFile("Chicago_Crimes_2012_to_2017_pre.csv")

val header=data_14.first()

val pre_data_1=data_14.filter(row=>row!=header)
val pre_data_2=data_57.filter(row=>row!=header)
val pre_data_3=data_811.filter(row=>row!=header)
val pre_data_4=data_1217.filter(row=>row!=header)

val dataRDD_1=pre_data_1.map{l=>
 val s=l.split('\t')
 (s(1),s(2),s(3),s(4),s(5),s(6),s(7))}

val dataRDD_2=pre_data_2.map{l=>
 val s=l.split('\t')
 (s(1),s(2),s(3),s(4),s(5),s(6),s(7))}

val dataRDD_3=pre_data_3.map{l=>
 val s=l.split('\t')
 (s(1),s(2),s(3),s(4),s(5),s(6),s(7))}

val dataRDD_4=pre_data_4.map{l=>
 val s=l.split('\t')
 (s(1),s(2),s(3),s(4),s(5),s(6),s(7))}
 
val dataRDD= dataRDD_1 union dataRDD_2 union dataRDD_3 union dataRDD_4
 
val year_crime=dataRDD.map{case(a,b,c,d,e,f,g)=>((c,f),1)}.reduceByKey(_+_).map{case((a,b),c)=>(a,b,c)}.sortBy(_._2)
val theft_year_count=year_crime.filter(row=>row._1=="THEFT")
val narcotics_year_count=year_crime.filter(row=>row._1=="NARCOTICS")
val assault_year_count=year_crime.filter(row=>row._1=="ASSAULT")
val burgulary_year_count=year_crime.filter(row=>row._1=="BURGLARY")
val motor_year_count=year_crime.filter(row=>row._1=="MOTOR VEHICLE THEFT")

def dumpToCSV[S](a:Array[S],fileName:String){
      val file=new java.io.PrintStream(fileName)
     
      a.foreach{l =>
                val str=l.toString.replaceAll("\\(","").replaceAll("\\)","")
                file.println(str)
               }
          file.close
      }


val year_crime_count=dataRDD.map{case(a,b,c,d,e,f,g)=>(f,1)}.reduceByKey(_+_).sortBy(_._1).toDF.show

val block_crime_count=dataRDD.map{case(a,b,c,d,e,f,g)=>((b,g),1)}.reduceByKey(_+_).sortBy(_._2)

val loc_count=dataRDD.map{case(a,b,c,d,e,f,g)=>(g,1)}.reduceByKey(_+_).sortBy(_._2,false)
dumpToCSV(robbery_location.collect,"Robbery.csv")

val assault_locations=dataRDD.filter(row=>row._3.contains("ASSAULT")).map{case(a,b,c,d,e,f,g)=>((b,g),1)}.reduceByKey(_+_).sortBy(_._2,false)


val crime_count=dataRDD.map{case(a,b,c,d,e,f,g,h,i)=>(d,1)}.reduceByKey(_+_)
crime_count.sortBy(_._2,false).toDF.show()


val robbery_location_time=dataRDD.filter(row=>row._3=="MOTOR VEHICLE THEFT").map{case(a,b,c,d,e,f,g)=>((b,g,a.split(" ")(1)),1)}.reduceByKey(_+_).sortBy(_._2,false)

val burgulary_loc_place_time=dataRDD.filter(row=>row._3=="BURGLARY").map{case(a,b,c,d,e,f,g)=>((g,e,a.split(" ")(1)+a.split(" ")(2)),1)}.reduceByKey(_+_).sortBy(_._2,false)

val crimes_near_police_station=dataRDD.filter(row=>row._5.contains("POLICE")).map{case(a,b,c,d,e,f,g)=>(f,1)}.reduceByKey(_+_).sortBy(_._2,false) 

val false_police_report=dataRDD.filter(row=>row._4=="FALSE POLICE REPORT").count

val school_children_abduction=dataRDD.filter(row=>row._5.contains("SCHOOL")).filter(row=>row._4.contains("CHILD ABDUCTION")).map{case(a,b,c,d,e,f,g)=>(f,1)}.reduceByKey(_+_).sortBy(_._1) // bar

val cyberstalking_year=dataRDD.filter(row=>row._4.contains("CYBERSTALKING")).map{case(a,b,c,d,e,f,g)=>(f,1)}.reduceByKey(_+_).sortBy(_._1) //bar

val bogus_check=dataRDD.filter(row=>row._4.contains("BOGUS CHECK")).map{case(a,b,c,d,e,f,g)=>(f,1)}.reduceByKey(_+_).sortBy(_._1) 

val counterfeit_doc=dataRDD.filter(row=>row._4=="COUNTERFEITING DOCUMENT").map{case(a,b,c,d,e,f,g)=>(f,1)}.reduceByKey(_+_).sortBy(_._1)

val bogus_check=dataRDD.filter(row=>row._4.contains("BOGUS CHECK")).map{case(a,b,c,d,e,f,g)=>(a.split(" ")(0),1)}.reduceByKey(_+_).sortBy(_._2,false)

val narcotics_mapping=dataRDD.filter(row=>row._3=="NARCOTICS").map{case(a,b,c,d,e,f,g)=>(g,1)}.reduceByKey(_+_).sortBy(_._2,false)

val school_loc=dataRDD.filter(row=>row._5.contains("SCHOOL")).map{case(a,b,c,d,e,f,g)=>(g,1)}.reduceByKey(_+_).sortBy(_._2,false)


val crime_2001_2004=dataRDD.filter(row=>row._6>=2001 && row._6<=2004).map{case(a,b,c,d,e,f,g)=>(c,1)}.reduceByKey(_+_).sortBy(_._2,false).filter(row=>row._2>50000)

val crime_2005_2007=dataRDD.filter(row=>row._6>=2005 && row._6<=2007).map{case(a,b,c,d,e,f,g)=>(c,1)}.reduceByKey(_+_).sortBy(_._2,false).filter(row=>row._2>50000)

val crime_2008_2011=dataRDD.filter(row=>row._6>=2008 && row._6<=2011).map{case(a,b,c,d,e,f,g)=>(c,1)}.reduceByKey(_+_).sortBy(_._2,false).filter(row=>row._2>50000)

val crime_2012_2017=dataRDD.filter(row=>row._6>=2012 && row._6<=2017).map{case(a,b,c,d,e,f,g)=>(c,1)}.reduceByKey(_+_).sortBy(_._2,false).filter(row=>row._2>50000)


val robbery_location=dataRDD.filter(row=>row._3=="MOTOR VEHICLE THEFT").map{case(a,b,c,d,e,f,g)=>(g,1)}.reduceByKey(_+_).sortBy(_._2,false)
dumpToCSV(robbery_location.collect,"Robbery.csv")
