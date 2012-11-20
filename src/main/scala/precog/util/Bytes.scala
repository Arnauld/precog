package precog.util

/**
 *
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
case class Bytes(raw:Array[Byte]) {
  private lazy val hash = java.util.Arrays.hashCode(raw)
  override def hashCode() = hash

  override def equals(obj: Any) =
    obj.isInstanceOf[Bytes] && BytesComparator.compare(obj.asInstanceOf[Bytes].raw, raw) == 0

  override def toString = "[" + raw.map("%02X" format _).mkString + "]"
}
