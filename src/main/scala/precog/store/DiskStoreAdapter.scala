package precog.store

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class DiskStoreAdapter(valueStore:BinaryStore,
                       indexStorage:IndexStore) extends DiskStore {
  def put(key: Array[Byte], value: Array[Byte]) {
    val address = valueStore.addressOf(value)
    indexStorage.put(key, address)
  }

  def get(key: Array[Byte]) =
    indexStorage.get(key) match {
      case Some(address) => valueStore.get(address)
      case _ => None
    }

  def flush() {
    // flush values first as it is required to ensure index's consistency
    // all `Address` must be resolved
    valueStore.flush()
    // ... then attempt to flush the index, that's not a big deal to store
    // the value is not used, but bad thing to store the key without any value
    indexStorage.flush()
  }

  def wrapReader[A](reader: Reader[A]): IndexReader[A] =
    reader match {
      case Done(v) => IndexDone(v)
      case More(f) => IndexMore({(x:Option[(Array[Byte], Address)]) =>
        x match {
          case None => wrapReader[A](f(None))
          case Some(t) =>
            val kv = (t._1, valueStore.get(t._2).get)
            val nextReader = f(Some(kv))
            wrapReader[A](nextReader)
        }
      })
    }

  def traverse[A](start: Array[Byte], end: Array[Byte])(reader: Reader[A]) =
    indexStorage.query(start, end, wrapReader(reader))
}
