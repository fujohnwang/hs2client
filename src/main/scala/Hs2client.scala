package hs2client{
	import java.net._, java.util._, java.util.concurrent._, java.util.concurrent.atomic._, java.io._
	import org.slf4j._
	import scala.util.control._

	case class Hs2Conf(
		host:String, 
		port4r:Int=9998, 
		port4wr:Int=9999, 
		connectionEncoding:String="utf-8", 
		readBufferSize:Int=1024*128, 
		connectTimeout:Int=10000,
		tcpNoDelay:Boolean=true,
		keepAlive:Boolean=true,
		reuseAddress:Boolean=true,
		soTimeout:Int=0,
		readerPoolSize:Int=12, 
		writerPoolSize:Int=1) 
	
	sealed abstract class Op(){def symbol():String}
	case class Eq(symbol:String="=") extends Op
	case class Gt(symbol:String=">") extends Op
	case class Lt(symbol:String="<") extends Op
	case class GE(symbol:String=">=") extends Op
	case class LE(symbol:String="<=") extends Op
	case class Ins(symbol:String="+")  extends Op
	
	case class Hs2Result(errorCode:Int, columnNumber:Int, columns:Array[String])
	
	case class OpenIndexSpec(db:String, table:String, columns:Array[String], index:String="PRIMARY")
	{
		val logger = LoggerFactory.getLogger(getClass)
		def toBytes(indexId:Int, encoding:String):Array[Byte] = {
			val buf = new StringBuilder("P")
			buf.append("\t").append(indexId).append("\t").append(db).append("\t").append(table).append("\t").append(index).append("\t")
			columns.foreach(buf.append(_).append(","))
			buf.deleteCharAt(buf.length-1)
			buf.append("\n")
			logger.debug("open spec string: '{}'", buf.toString)
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
	
	trait ConnectionHolder {
		def connection():Socket
		def connectionEncoding():String
		def readBufferSize():Int
		def connectionClosed():Unit
	}
	

	case class SimpleConnectionHolder(connection:Socket, connectionEncoding:String, readBufferSize:Int) extends ConnectionHolder {
		def connectionClosed(){
			connection.close
		}
	} 
	
	
 	class Hs2SessionImpl(indexId:Int, spec:OpenIndexSpec,connHolder:ConnectionHolder) extends Hs2Session{
		val logger = LoggerFactory.getLogger(getClass)
		def indexId():Int = {indexId}
		def get(op:Op, indexValues:Array[String], limit:Int=1, offset:Int=0):Array[String] = {
			val buf = new StringBuilder() // don't use number as constructor argument of stringBuilder, otherwise, it may ignore it.
			buf.append(indexId).append("\t").append(op.symbol).append("\t").append(indexValues.length).append("\t")
			indexValues.foreach(v=>{
				if(v == null) buf.append(0x00) else buf.append(v)
				buf.append("\t")
			})
			buf.append(limit).append("\t").append(offset).append("\n")
			logger.debug("query string:{}", buf.toString)
			E.writeTo(buf.toString.getBytes(connHolder.connectionEncoding), connHolder.connection.getOutputStream)
			val resultBytes =E.readFrom(connHolder.connection.getInputStream, connHolder.readBufferSize) 
			val result = E.assembly(resultBytes, connHolder.connectionEncoding)
			if(result.errorCode != 0){throw new RuntimeException("failed to query with server response:"+result)}
			result.columns
		}
		def update(op:Op, indexValues:Array[String], colValues:Array[String], limit:Int=1, offset:Int=0, mop:Char='U'):Int = {
			val buf = new StringBuilder()
			buf.append(indexId)
				.append("\t")
				.append(op.symbol)
				.append("\t")
				.append(indexValues.length)
				.append("\t");
			indexValues.foreach(v=>{
				if(v == null) buf.append(0x00) else buf.append(v)
				buf.append("\t")
			})
			buf.append(limit)
				.append("\t")
				.append(offset)
				.append("\t")
				.append(mop)
				.append("\t");
			colValues.foreach(v=>{
				if(v== null)buf.append(0x00) else buf.append(v)
				buf.append("\t")
			})
			buf.deleteCharAt(buf.length-1)
			buf.append("\n")
			// write command 
			E.writeTo(buf.toString.getBytes(connHolder.connectionEncoding), connHolder.connection.getOutputStream)
			val resultBytes =E.readFrom(connHolder.connection.getInputStream, connHolder.readBufferSize) 
			val result = E.assembly(resultBytes, connHolder.connectionEncoding)
			if(result.errorCode != 0){throw new RuntimeException("failed to update with server response:"+result)}
			result.columns(0).toInt
		}
		def delete(op:Op, indexValues:Array[String], limit:Int=1, offset:Int=0, mop:Char='D'):Int ={
			val buf = new StringBuilder()
			buf.append(indexId)
				.append("\t")
				.append(op.symbol)
				.append("\t")
				.append(indexValues.length)
				.append("\t");
			indexValues.foreach(v=>{
				if(v == null) buf.append(0x00) else buf.append(v)
				buf.append("\t")
			})
			buf.append(limit)
				.append("\t")
				.append(offset)
				.append("\t")
				.append(mop)
				.append("\n");
			E.writeTo(buf.toString.getBytes(connHolder.connectionEncoding), connHolder.connection.getOutputStream)
			val resultBytes =E.readFrom(connHolder.connection.getInputStream, connHolder.readBufferSize) 
			val result = E.assembly(resultBytes, connHolder.connectionEncoding)
			if(result.errorCode != 0){throw new RuntimeException("failed to delete with server response:"+result)}
			result.columns(0).toInt
		}
		def insert(colValues:Array[String], op:Op=Ins()):Int = {
			val buf = new StringBuilder
			buf.append(indexId)
				.append("\t")
				.append(op.symbol)
				.append("\t")
				.append(colValues.length)
				.append("\t");
			colValues.foreach(v=>{
				if(v == null) buf.append(0x00) else buf.append(v)
				buf.append("\t")
			})
			buf.deleteCharAt(buf.length-1)
			buf.append("\n")
			
			E.writeTo(buf.toString.getBytes(connHolder.connectionEncoding), connHolder.connection.getOutputStream)
			val resultBytes =E.readFrom(connHolder.connection.getInputStream, connHolder.readBufferSize) 
			val result = E.assembly(resultBytes, connHolder.connectionEncoding)
			if(result.errorCode != 0){throw new RuntimeException("failed to insert with server response:"+result)}
			result.columnNumber
		}
		def close(){
			connHolder.connectionClosed
		}
	} 
	
	class Hs2ReadOnlySessionImpl(indexId:Int, spec:OpenIndexSpec, connHolder:ConnectionHolder) extends Hs2SessionImpl(indexId,spec, connHolder){
		override def update(op:Op, indexValues:Array[String], colValues:Array[String], limit:Int=1, offset:Int=0, mop:Char='U'):Int ={
			throw new UnsupportedOperationException("update is not supported in a readonly sesion");
		}
		override def delete(op:Op, indexValues:Array[String], limit:Int=1, offset:Int=0, mop:Char='D'):Int ={
			throw new UnsupportedOperationException("delete is not supported in a readonly sesion");
		}
		override def insert(colValues:Array[String], op:Op=Ins()):Int ={
			throw new UnsupportedOperationException("insert is not supported in a readonly sesion");
		}
	}
	
	trait Hs2SessionFactory {
		def openSession(openIndexSpec:OpenIndexSpec, readOnly:Boolean=false):Hs2Session
	}
	/* currently, no pooling of connection is implemented, will be added in the near future.*/
	class Hs2SessionFactoryImpl(conf:Hs2Conf) extends Hs2SessionFactory {
		val indexIdCache = new ConcurrentHashMap[OpenIndexSpec,Int]()
		val connectionCache = new ConcurrentHashMap[Socket, AtomicInteger]()
		val logger = LoggerFactory.getLogger(getClass)
		def openSession(spec:OpenIndexSpec, readOnly:Boolean=false):Hs2Session = {
			var conn:Socket = null;
			if(readOnly){
				logger.info("open read only session for:{}.{}", spec.db, spec.table)
				conn = getReadOnlyConnection()
			}
			else{
				logger.info("open read/write session for:{}.{}", spec.db, spec.table)
				conn = getConnection()
			}
			connectionCache.putIfAbsent(conn,new AtomicInteger(0))
			val indexId = connectionCache.get(conn).getAndIncrement()
			indexIdCache.putIfAbsent(spec, indexId)
			logger.info("write open index request to hs server side.")
			E.writeTo(spec.toBytes(indexId, conf.connectionEncoding),  conn.getOutputStream)
			val resultBytes = E.readFrom(conn.getInputStream, conf.readBufferSize)
			val result = E.assembly(resultBytes, conf.connectionEncoding)
			if(result.errorCode != 0){
				conn.close()
				throw new RuntimeException("failed to open index with result:"+result);
			}
			val connHolder = new SimpleConnectionHolder(conn, conf.connectionEncoding, conf.readBufferSize)
			if(readOnly){new Hs2ReadOnlySessionImpl(indexId, spec, connHolder)}else{new Hs2SessionImpl(indexId, spec, connHolder)}
		}
		
		def getConnection():Socket = {
			val socket = new Socket
			socket.setTcpNoDelay(conf.tcpNoDelay)
			socket.setKeepAlive(conf.keepAlive)
			socket.setReuseAddress(conf.reuseAddress)
			socket.setSoTimeout(conf.soTimeout)
			socket.connect(new InetSocketAddress(conf.host, conf.port4wr), conf.connectTimeout)
			socket
		}
		
		def getReadOnlyConnection():Socket = {
			val socket = new Socket
			socket.setTcpNoDelay(conf.tcpNoDelay)
			socket.setKeepAlive(conf.keepAlive)
			socket.setReuseAddress(conf.reuseAddress)
			socket.setSoTimeout(conf.soTimeout)
			socket.connect(new InetSocketAddress(conf.host, conf.port4r), conf.connectTimeout)
			socket
		}
	}
	
	object E{
		val logger = LoggerFactory.getLogger(getClass)
		def encode(line:Array[Byte]):Array[Byte] = {
			val out = new ByteArrayOutputStream()
			line.foreach(b=>{
				if(b >=0x10 || b <=0xff){
					out.write(b)
				}else if(b >=0x00 || b <=0x0f){
					out.write(0x01)
					out.write(0x40|b)
				}else{
					assert(false)
				}
			})
			out.toByteArray
		}
		def writeTo(bytes:Array[Byte], out:OutputStream):Unit={
			val writer = new DataOutputStream(new BufferedOutputStream(out))
			writer.write(encode(bytes))
			writer.flush
			logger.debug("write of command bytes is done.")
		}
		
		def readFrom(ins:InputStream, readBufferSize:Int = 1024*128):Array[Byte] = {
			val reader = new DataInputStream(new BufferedInputStream(ins, readBufferSize))
			var buffer = new Array[Byte](readBufferSize)
			val out = new ByteArrayOutputStream()
			var size = 0
			val breaks = new Breaks
			import breaks.{break, breakable}
			breakable{
				while(true){
					logger.debug("read data to buffer.")
					size = reader.read(buffer)
					logger.debug("write part of result to buffer")
					out.write(buffer, 0, size)
					if(size < readBufferSize){
						logger.debug("break the loop in result reading")
						break
					}
				}
			}
			filter(out.toByteArray)
		}
		
		def filter(bytes:Array[Byte]) : Array[Byte] = {
			val out = new ByteArrayOutputStream
			var i = 0;
			while(i<bytes.length){
				if(bytes(i) == 0x01){
					out.write(bytes(i+1)^0x40)
					i=i+2;
				}else{
					out.write(bytes(i))
					i=i+1;
				}
			}
			out.toByteArray
		}
		/* 
		* 1. split the result 
		* 2. decode the column values
		* NOTE: only the last column's value allows to have special characters like tab or linefeed.
		*/
		def assembly(bytes:Array[Byte], encoding:String):Hs2Result = {
			var index = 0
			val errorCodeBytes = readToken(bytes, index); index=index+errorCodeBytes.length+1;
			val errorCode = new String(filter(errorCodeBytes), encoding)
			
			val columnNumberBytes = readToken(bytes, index); index = index+columnNumberBytes.length;
			val columnNumber = new String(filter(columnNumberBytes), encoding)
			
			if(bytes(index) == 0x0a){
				return Hs2Result(errorCode.toInt, columnNumber.toInt, Array[String]())
			}
			index = index+1;
			
			val columns:Array[String] = new Array[String](columnNumber.toInt)
			for(i<- 0 until columns.length-1){
				val columnBytes = readToken(bytes, index); index = index + columnBytes.length+1;
				columns(i) = new String(filter(columnBytes), encoding)
			}
			val leftBytes = new Array[Byte](bytes.length - index-1)
			System.arraycopy(bytes, index, leftBytes, 0, leftBytes.length)
			columns(columns.length -1 ) = new String(filter(leftBytes), encoding)
			Hs2Result(errorCode.toInt, columnNumber.toInt, columns)
		}
		
		def readToken(bytes:Array[Byte], idx:Int):Array[Byte] = {
			val buffer = new ByteArrayOutputStream
			var index = idx
			while(!(bytes(index) == 0x09 || bytes(index) == 0x0a)){
				buffer.write(bytes(index))
				index = index+1;
			}
			index = index+1;// bypass delimeter
			buffer.toByteArray
		}
	}
	
	/* wrap the template behavior with hs2client usage, but not so effecient as raw usage since it will open/close session frequently.*/
	class Hs2ClientTemplate(){
		def get(openIndexSpec:OpenIndexSpec, op:Op, indexValues:Array[String], limit:Int=1, offset:Int=0):Array[String] = {
			null
		}
		def update(openIndexSpec:OpenIndexSpec, op:Op, indexValues:Array[String], colValues:Array[String], limit:Int=1, offset:Int=0, mop:Char='U'):Int = {
			0
		}
		def delete(openIndexSpec:OpenIndexSpec, op:Op, indexValues:Array[String], limit:Int=1, offset:Int=0, mop:Char='D'):Int ={
			0
		}
		def insert(openIndexSpec:OpenIndexSpec, colValues:Array[String], op:Op=Ins()):Int = {
			0
		}
	}
	
}
