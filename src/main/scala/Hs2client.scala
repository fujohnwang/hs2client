package hs2client{
	import java.net._, java.util._, java.util.concurrent._, java.util.concurrent.atomic._, java.io._
	import org.slf4j._

	case class Hs2Conf(host:String, port4r:Int=9998, port4wr:Int=9999, connectionEncoding:String="utf-8", readBuffer:Int=4096) 
	
	sealed abstract class Op(){
		def symbol():String
	}
	case class Eq(symbol:String="=") extends Op
	case class Gt(symbol:String=">") extends Op
	case class Lt(symbol:String="<") extends Op
	case class GE(symbol:String=">=") extends Op
	case class LE(symbol:String="<=") extends Op
	case class Ins(symbol:String="+")  extends Op
	
	case class OpenIndexSpec(db:String, table:String, columns:Array[String], index:String="PRIMARY")
	{
		def toBytes(indexId:Int, encoding:String):Array[Byte] = {
			val buf = new StringBuilder("P")
			buf.append(0x09).append(indexId).append(0x09).append(db).append(0x09).append(table).append(0x09).append(index).append(0x09)
			columns.foreach(buf.append(_).append(","))
			buf.deleteCharAt(buf.length-1)
			buf.append(0x0a)
			buf.toString.getBytes(encoding)
		}
	}
	
	trait Hs2Session {
		def indexId():Int
		def get(op:Op, indexValues:Array[String], limit:Int=1, offset:Int=0):Array[String]
		def update(op:Op, indexValues:Array[String], colValues:Array[String], limit:Int=1, offset:Int=0, mop:Char='U'):Int
		def delete(op:Op, indexValues:Array[String], limit:Int=1, offset:Int=0, mop:Char='D'):Int
		def insert(colValues:Array[String], op:Op=Ins()):Int
		def close():Unit
	}
	
	
	case class Hs2SessionImpl(indexId:Int, spec:OpenIndexSpec, readOnly:Boolean, conn:Hs2Connection) extends Hs2Session{
		def get(op:Op, indexValues:Array[String], limit:Int=1, offset:Int=0):Array[String] = {
			val buf = new StringBuilder(indexId)
			buf.append(0x09).append(op.symbol).append(0x09).append(indexValues.length).append(0x09)
			indexValues.foreach(v=>{
				if(v == null) buf.append(0x00) else buf.append(v)
				buf.append(0x09)
			})
			buf.append(limit).append(0x09).append(offset).append(0x0a)
			val resultBytes = conn.execute(buf.toString.getBytes(conn.getConnectionEncoding), true)
			val resultLine = new String(resultBytes, conn.getConnectionEncoding)
			val parts:Array[String] = resultLine.split("\t")
			parts.foreach(s=>println("[]"+s+"]"))
			null
		}
		def update(op:Op, indexValues:Array[String], colValues:Array[String], limit:Int=1, offset:Int=0, mop:Char='U'):Int = {
			0
		}
		def delete(op:Op, indexValues:Array[String], limit:Int=1, offset:Int=0, mop:Char='D'):Int ={
			0
		}
		def insert(colValues:Array[String], op:Op=Ins()):Int = {
			0
		}
		def close(){
			conn.close
		}
	} 
	
	trait Hs2SessionFactory {
		def openSession(openIndexSpec:OpenIndexSpec, readOnly:Boolean=false):Hs2Session
	}
	
	class Hs2SessionFactoryImpl(cp:Hs2ConnectionProvider) extends Hs2SessionFactory {
		val indexIdCache = new ConcurrentHashMap[OpenIndexSpec,Int]()
		val connectionCache = new ConcurrentHashMap[Hs2Connection, AtomicInteger]()
		val logger = LoggerFactory.getLogger(getClass)
		def openSession(spec:OpenIndexSpec, readOnly:Boolean=false):Hs2Session = {
			val conn = cp.getConnection
			connectionCache.putIfAbsent(conn,new AtomicInteger(0))
			val indexId = connectionCache.get(conn).getAndIncrement()
			indexIdCache.putIfAbsent(spec, indexId)
			val resultBytes = conn.execute(spec.toBytes(indexId, conn.getConnectionEncoding), readOnly);
			logger.debug("result bytes of open index:"+Arrays.toString(resultBytes))
			new Hs2SessionImpl(indexId, spec, readOnly, conn)
		}
	}
	
	trait Hs2Connection {
		def execute(line:Array[Byte], readOnly:Boolean):Array[Byte]
		def getConnectionEncoding():String
		def close():Unit
	}
	
	class Hs2ConnectionImpl(wr:Socket, ro:Socket, bufferSize:Int, encoding:String) extends Hs2Connection  {
		val logger = LoggerFactory.getLogger(getClass)
		def execute(line:Array[Byte], readOnly:Boolean):Array[Byte] = {
			var writer:OutputStream = null
			var reader:InputStream = null
			if(readOnly){
				writer = ro.getOutputStream;reader = new BufferedInputStream(ro.getInputStream)
			}else{
				writer = wr.getOutputStream;reader = new BufferedInputStream(wr.getInputStream)
			}
			logger.debug("start to write request to hs server side...")
			writer.write(E.encode(line))
			writer.flush
			logger.debug("request is sent.")
			val out = new ByteArrayOutputStream
			
			logger.debug("try to read response...")
			var singleByte = 0x00;
			do{
				singleByte = reader.read()
				println(">"+singleByte)
				if(singleByte == 0x01){
					singleByte = reader.read()
					out.write(0x40^singleByte)
				}
				else{
					out.write(singleByte)
				}
			}while(singleByte != 0x0a);
			logger.debug("response is received.")
			out.toByteArray
		}
		
		def getConnectionEncoding():String = {
			encoding
		}
		def close(){
			wr.close
			ro.close
		}
	}
	
	trait Hs2ConnectionProvider {
		def getConnection():Hs2Connection
		def returns(conn:Hs2Connection):Unit
	}
	
	/* SHOULD BE EXTENDED LATER TO ENABLE BIDIRECTIONAL COMMUNICATION VIA CONRESPONDING PORT*/
	class Hs2SimplePoolingConnectionProvider(poolSize:Int,conf:Hs2Conf) extends Hs2ConnectionProvider  {
		// val semaphore = new Semaphore(poolSize, true)
		val logger       = LoggerFactory.getLogger(getClass)
		
		
		def getConnection():Hs2Connection = {
			val socket = new Socket()
			socket.connect(new InetSocketAddress(conf.host, conf.port4wr))
			val roSocket = new Socket()
			roSocket.connect(new InetSocketAddress(conf.host, conf.port4r))
			new Hs2ConnectionImpl(socket, roSocket, conf.readBuffer, conf.connectionEncoding)
		}
		def returns(conn:Hs2Connection){
			// do nothing for now
		}
		
		def destroy(){
			// do nothing for now
		}
	}
	
	/* wrap the template behavior with hs2client usage, but not so effecient as raw usage since it will open/close session frequently.*/
	class Hs2ClientTemplate(){
		
	}
	
	object E{
		def encode(line:Array[Byte]):Array[Byte] = {
			val out = new ByteArrayOutputStream()
			line.foreach(b=>{
				if(b >=0x10 && b <=0xff){
					out.write(b)
				}else if(b >=0x00 && b <=0x0f){
					out.write(0x01)
					out.write(0x40|b)
				}else{
					assert(false)
				}
			})
			out.toByteArray
		}
	}
	
	// trait for common logic
	// basic client with open index specs passed in 
	// extended client with open index specs fetched from database automatically
	// 
	
	object SandboxRunner {
	  def main(args: Array[String]): Unit = {
		val connectionProvider = new Hs2SimplePoolingConnectionProvider(1, Hs2Conf("10.16.201.39"))
		val sessionFactory = new Hs2SessionFactoryImpl(connectionProvider)
		val openIndexSpec = OpenIndexSpec("test", "dw", Array("id", "value"))
		val session = sessionFactory.openSession(openIndexSpec, true)
		try{
			session.get(Eq(),Array("1"))
		}
		finally{
			session.close
		}
	
	  }
	}
	
}
