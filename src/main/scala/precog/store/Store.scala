package precog.store

import precog.util.Bytes

/**
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */

case class Address(var offset:Long)



sealed trait IndexReader[B]
object IndexReader {
  case class More[B](value: Option[(Bytes, Address)] => IndexReader[B]) extends IndexReader[B]
  case class Done[B](value: B) extends IndexReader[B]
}


trait IndexStore {
  def query[B](start: Bytes, end: Bytes, reader:IndexReader[B]):B

  def flush(): Unit

  def get(key: Bytes): Option[Address]

  def put(key: Bytes, address: Address): Unit
}

trait BinaryStore {
  /**
   * Let's flush!
   */
  def flush()

  /**
   * Return the value actually stored at this {@link Address}.
   *
   * @param address address of the value
   * @return
   */
  def get(address: Address): Option[Array[Byte]]

  /**
   * Return the {@link Address} of the given value, if the value was already previously stored
   * then its {@link Address} is returned, otherwise a new {@link Address} is allocated to store the
   * value.
   * @param value that need to be stored
   * @return
   */
  def addressOf(value:Bytes):Address
}
