package precog.store

/**
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */

case class Address(var offset:Option[Long])

trait PageStore {
}

sealed trait IndexReader[B]
case class IndexMore[B](value: Option[(Array[Byte], Address)] => IndexReader[B]) extends IndexReader[B]
case class IndexDone[B](value: B) extends IndexReader[B]


trait IndexStore {
  def query[B](start: Array[Byte], end: Array[Byte], reader:IndexReader[B]):B

  def flush(): Unit

  def get(key: Array[Byte]): Option[Address]

  def put(key: Array[Byte], address: Address): Unit

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
  def addressOf(value:Array[Byte]):Address
}
