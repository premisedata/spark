package spark

import java.io._
import java.nio.ByteBuffer
import com.esotericsoftware.kryo.{Kryo, KryoException}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.serializers.{JavaSerializer => KryoJavaSerializer}
import com.twitter.chill.KryoBijection
import serializer.{SerializerInstance, DeserializationStream, SerializationStream}
import spark.broadcast._
import spark.storage._

private[spark] class KryoSerializationStream(kryo: Kryo, outStream: OutputStream) extends SerializationStream {
  val output = new KryoOutput(outStream)

  def writeObject[T](t: T): SerializationStream = {
    kryo.writeClassAndObject(output, t)
    this
  }

  def flush() { output.flush() }
  def close() { output.close() }
}

private[spark] class KryoDeserializationStream(kryo: Kryo, inStream: InputStream) extends DeserializationStream {
  val input = new KryoInput(inStream)

  def readObject[T](): T = {
    try {
      kryo.readClassAndObject(input).asInstanceOf[T]
    } catch {
      // DeserializationStream uses the EOF exception to indicate stopping condition.
      case _: KryoException => throw new EOFException
    }
  }

  def close() {
    // Kryo's Input automatically closes the input stream it is using.
    input.close()
  }
}

private[spark] class KryoSerializerInstance(ks: KryoSerializer) extends SerializerInstance {
  val kryo = ks.kryo.get()
  val output = ks.output.get()
  val input = ks.input.get()

  def serialize[T](t: T): ByteBuffer = {
    output.clear()
    kryo.writeClassAndObject(output, t)
    ByteBuffer.wrap(output.toBytes)
  }

  def deserialize[T](bytes: ByteBuffer): T = {
    input.setBuffer(bytes.array)
    kryo.readClassAndObject(input).asInstanceOf[T]
  }

  def deserialize[T](bytes: ByteBuffer, loader: ClassLoader): T = {
    val oldClassLoader = kryo.getClassLoader
    kryo.setClassLoader(loader)
    input.setBuffer(bytes.array)
    val obj = kryo.readClassAndObject(input).asInstanceOf[T]
    kryo.setClassLoader(oldClassLoader)
    obj
  }

  def serializeStream(s: OutputStream): SerializationStream = {
    new KryoSerializationStream(kryo, s)
  }

  def deserializeStream(s: InputStream): DeserializationStream = {
    new KryoDeserializationStream(kryo, s)
  }
}

/**
 * Interface implemented by clients to register their classes with Kryo when using Kryo
 * serialization.
 */
trait KryoRegistrator {
  def registerClasses(kryo: Kryo)
}

/**
 * A Spark serializer that uses the [[http://code.google.com/p/kryo/wiki/V1Documentation Kryo 1.x library]].
 */
class KryoSerializer extends spark.serializer.Serializer with Logging {
  private val bufferSize = System.getProperty("spark.kryoserializer.buffer.mb", "2").toInt * 1024 * 1024

  val kryo = new ThreadLocal[Kryo] {
    override def initialValue = newKryo()
  }

  val output = new ThreadLocal[KryoOutput] {
    override def initialValue = new KryoOutput(bufferSize)
  }

  val input = new ThreadLocal[KryoInput] {
    override def initialValue = new KryoInput(bufferSize)
  }

  def newKryo(): Kryo = {
    val kryo = KryoBijection.getKryo
    val classLoader = Thread.currentThread.getContextClassLoader

    // Register some commonly used classes
    val toRegister: Seq[AnyRef] = Seq(
      ByteBuffer.allocate(1),
      StorageLevel.MEMORY_ONLY,
      PutBlock("1", ByteBuffer.allocate(1), StorageLevel.MEMORY_ONLY),
      GotBlock("1", ByteBuffer.allocate(1)),
      GetBlock("1")
    )

    for (obj <- toRegister) kryo.register(obj.getClass)

    // Allow sending SerializableWritable
    kryo.register(classOf[SerializableWritable[_]], new KryoJavaSerializer())
    kryo.register(classOf[HttpBroadcast[_]], new KryoJavaSerializer())

    // Allow the user to register their own classes by setting spark.kryo.registrator
    try {
      Option(System.getProperty("spark.kryo.registrator")).foreach { regCls =>
        logInfo("Running user registrator: " + regCls)
        val reg = Class.forName(regCls, true, classLoader).newInstance().asInstanceOf[KryoRegistrator]
        reg.registerClasses(kryo)
      }
    } catch {
      case _: Exception => println("Failed to register spark.kryo.registrator")
    }

    kryo.setClassLoader(classLoader)
    kryo
  }

  def newInstance(): SerializerInstance = {
    new KryoSerializerInstance(this)
  }
}
