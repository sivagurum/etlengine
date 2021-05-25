package com.aero.utils

object implicits extends Serializable{

    //implicit functions for internal conversions
    //    implicit def jValue2Tuple4(in: JValue): scala.Tuple4[Any, String, Any, Any] = {
    //      val inList = in.values.asInstanceOf[List[String]]
    //      (inList(0), inList(1).toString().toLowerCase(), inList(2), if (inList.size < 4) "" else inList(3))
    //    }

    //    //implicit functions for internal conversions
    //    implicit def any2Tuple4(in: Any): scala.Tuple4[Any, String, Any, Any] = {
    //      val inList = in.values.asInstanceOf[List[String]]
    //      (inList(0), inList(1).toString().toLowerCase(), inList(2), if (inList.size < 4) "" else inList(3))
    //    }

    //    //implicit functions for list to tuple conversions
    //    implicit def jListTuple4(inList: List[String]): scala.Tuple4[Any, String, Any, Any] = {
    //      (inList(0), inList(1).toString().toLowerCase(), inList(2), if (inList.size < 4) "" else inList(3))
    //    }

    implicit def anyToMap(in: Any): Map[String, String] = { in.asInstanceOf[Map[String, String]] }
    implicit def anyToMap2(in: Any): String = { in.toString }
    implicit val name = "No name supplied"

    //def getString(in: String)(implicit map: Map[String, String]): String = { map(in) }
  
}