object SandboxRunner {
  import hs2client._
  def main(args: Array[String]): Unit = {
	val sessionFactory = new Hs2SessionFactoryImpl(Hs2Conf("10.16.201.39",connectionEncoding="GBK"))
	/*
	* open a readOnly session for read
	*/
	val session = sessionFactory.openSession(OpenIndexSpec("test", "dw", Array("id", "value")), true)
	try{
		println("result: "+session.get(Eq(),Array("13")).next.columns.mkString(","))
		// or
		val result = session.get(Eq(),Array("13"))
		if(result.hasNext()){
			val row = result.next
			println("column value of 1 : "+row.column(1))
		}
	}
	finally{
		session.close
	}
	
	/*
	* open a read/write session for update
	*/
	val wrsession = sessionFactory.openSession(OpenIndexSpec("test", "dw", Array("value")))
	try{
		wrsession.insert(Array("17", "darren wang"))
		wrsession.update(Eq(), Array("17"), Array("fujohnwang"))
		wrsession.delete(Eq(), Array("17"))
	}
	finally{
		wrsession.close
	}
  }
}