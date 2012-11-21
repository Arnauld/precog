package precog.util

object BytesImplicits {
  /**
   * Convert a <code>String</code> to an array of bytes using its <code>utf-8</code> representation.
   */
  implicit def bytes(s:String):Array[Byte] = s.getBytes("utf-8")

  /**
   * Convert an <code>int</code> to an array of 4 bytes, high byte first.
   */
  implicit def bytes(v:Int):Array[Byte]    = Array(
                                             ((v >>> 24) & 0xFF).asInstanceOf[Byte],
                                             ((v >>> 16) & 0xFF).asInstanceOf[Byte],
                                             ((v >>>  8) & 0xFF).asInstanceOf[Byte],
                                             ((v >>>  0) & 0xFF).asInstanceOf[Byte])

  /**
   * Convert an <code>long</code> to an array of 8 bytes, high byte first.
   */
  implicit def bytes(v:Long):Array[Byte]   = Array(
                                              ((v >>> 56) & 0xFF).asInstanceOf[Byte],
                                              ((v >>> 48) & 0xFF).asInstanceOf[Byte],
                                              ((v >>> 40) & 0xFF).asInstanceOf[Byte],
                                              ((v >>> 32) & 0xFF).asInstanceOf[Byte],
                                              ((v >>> 24) & 0xFF).asInstanceOf[Byte],
                                              ((v >>> 16) & 0xFF).asInstanceOf[Byte],
                                              ((v >>>  8) & 0xFF).asInstanceOf[Byte],
                                              ((v >>>  0) & 0xFF).asInstanceOf[Byte])
}


/**
 *
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
case class Bytes(raw:Array[Byte]) {
  def length() = raw.length

  private lazy val hash = java.util.Arrays.hashCode(raw)
  override def hashCode() = hash

  override def equals(obj: Any) =
    obj.isInstanceOf[Bytes] && BytesComparator.compare(obj.asInstanceOf[Bytes].raw, raw) == 0

  override def toString = "[" + raw.map("%02X" format _).mkString + "]"
}
