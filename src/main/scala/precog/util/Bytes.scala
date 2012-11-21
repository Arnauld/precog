package precog.util


object BytesImplicits {
  /**
   * Convert a <code>String</code> to an array of bytes using its <code>utf-8</code> representation.
   */
  implicit def bytes(s: String): Array[Byte] = s.getBytes("utf-8")

  /**
   * Convert an <code>int</code> to an array of 4 bytes, high byte first.
   */
  implicit def bytes(v: Int): Array[Byte] = {
    val bytes = new Array[Byte](4)
    Bytes.writeInt(0, bytes, v)
    bytes
  }

  /**
   * Convert an <code>long</code> to an array of 8 bytes, high byte first.
   */
  implicit def bytes(v: Long): Array[Byte] = {
    val bytes = new Array[Byte](8)
    Bytes.writeLong(0, bytes, v)
    bytes
  }
}


/**
 *
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
object Bytes {
  def apply(raw: Array[Byte]) = new Bytes(raw)

  def readInt(offset: Int, bytes: Array[Byte]): Int =
    ((bytes(offset + 0) & 0xFF) << 24) +
      ((bytes(offset + 1) & 0xFF) << 16) +
      ((bytes(offset + 2) & 0xFF) << 8) +
      ((bytes(offset + 3) & 0xFF) << 0)

  def readLong(offset: Int, bytes: Array[Byte]): Long =
    ((bytes(offset + 0).asInstanceOf[Long] << 56) +
      ((bytes(offset + 1) & 0xFF).asInstanceOf[Long] << 48) +
      ((bytes(offset + 2) & 0xFF).asInstanceOf[Long] << 40) +
      ((bytes(offset + 3) & 0xFF).asInstanceOf[Long] << 32) +
      ((bytes(offset + 4) & 0xFF).asInstanceOf[Long] << 24) +
      ((bytes(offset + 5) & 0xFF) << 16) +
      ((bytes(offset + 6) & 0xFF) << 8) +
      ((bytes(offset + 7) & 0xFF) << 0))

  def writeLong(offset: Int, bytes: Array[Byte], v: Long) {
    bytes(offset + 0) = ((v >>> 56) & 0xFF).asInstanceOf[Byte]
    bytes(offset + 1) = ((v >>> 48) & 0xFF).asInstanceOf[Byte]
    bytes(offset + 2) = ((v >>> 40) & 0xFF).asInstanceOf[Byte]
    bytes(offset + 3) = ((v >>> 32) & 0xFF).asInstanceOf[Byte]
    bytes(offset + 4) = ((v >>> 24) & 0xFF).asInstanceOf[Byte]
    bytes(offset + 5) = ((v >>> 16) & 0xFF).asInstanceOf[Byte]
    bytes(offset + 6) = ((v >>> 8) & 0xFF).asInstanceOf[Byte]
    bytes(offset + 7) = ((v >>> 0) & 0xFF).asInstanceOf[Byte]
  }

  def writeInt(offset: Int, bytes: Array[Byte], v: Int) {
    bytes(offset + 0) = ((v >>> 24) & 0xFF).asInstanceOf[Byte]
    bytes(offset + 1) = ((v >>> 16) & 0xFF).asInstanceOf[Byte]
    bytes(offset + 2) = ((v >>> 8) & 0xFF).asInstanceOf[Byte]
    bytes(offset + 3) = ((v >>> 0) & 0xFF).asInstanceOf[Byte]
  }
}

class Bytes(val raw: Array[Byte]) {
  def length() = raw.length

  private lazy val hash = java.util.Arrays.hashCode(raw)

  override def hashCode() = hash

  override def equals(obj: Any) =
    obj.isInstanceOf[Bytes] && BytesComparator.compare(obj.asInstanceOf[Bytes].raw, raw) == 0

  override def toString = "[" + raw.map("%02X" format _).mkString + "]"
}

object BytesRandomAcess {
  def apply(bytes: Array[Byte], inuse: Int) = new BytesRandomAcess {
    private var _writeOffset = inuse

    override def append(content: Array[Byte]):Long = {
      val offset = _writeOffset
      _writeOffset += content.length
      System.arraycopy(content, 0, bytes, offset, content.length)
      offset
    }

    override def read(offset: Int, amount: Int, dst: Array[Byte]) {
      System.arraycopy(bytes, offset, dst, 0, amount)
    }
  }
}

trait BytesRandomAcess {
  def append(content: Array[Byte]): Long

  def read(offset: Int, amount: Int, dst: Array[Byte])
}
