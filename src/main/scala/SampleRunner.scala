object SandboxRunner {
  import hs2client._
  def main(args: Array[String]): Unit = {
	val sessionFactory = new Hs2SessionFactoryImpl(Hs2Conf("10.16.201.39",connectionEncoding="GBK"))
	val session = sessionFactory.openSession(OpenIndexSpec("test", "dw", Array("id", "value")), true)
	try{
		println("result: "+session.get(Eq(),Array("13")).mkString(","))
	}
	finally{
		session.close
	}
	
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