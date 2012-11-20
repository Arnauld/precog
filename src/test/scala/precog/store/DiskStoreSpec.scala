package precog.store

import org.specs2.mutable.Specification
import annotation.tailrec
import precog.util.{Bytes, BytesComparator}
import java.util.concurrent.atomic.AtomicLong
import scala.Array

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class DiskStoreSpec extends Specification {

  implicit def bytes(s:String) = s.getBytes("utf-8")

  "DiskStore" should {
    "be sure that Duplicate values cannot be stored more than once" in {
      /*
      That is, if you add 10000 keys all with the value "John Doe",
      then the text "John Doe" must be stored only a single time.
      */
      val valueStore = new BasicBinaryStore()
      val indexStorage = new BasicIndexStore()

      val store = new DiskStoreAdapter(valueStore, indexStorage)

      store.put("1", "John Doe")
      store.put("2", "John Doe")
      store.put("3", "John Doe")
      store.put("4", "John Doe")
      store.put("5", "John Doe")

      valueStore.addresses.toList must contain(Bytes("John Doe") -> Address(Some(0))).only

    }

  }
}



class BasicBinaryStore extends BinaryStore {
  var addresses:Map[Bytes, Address] = Map()
  private val offset = new AtomicLong()

  override def flush() {}
  override def get(address: Address) = addresses.find(_._2 == address).map(_._1.raw)
  override def addressOf(value: Array[Byte]) = {
    val bytes = Bytes(value)
    addresses.get(bytes) match {
      case Some(addr) => addr
      case None =>
        val address = Address(Some(offset.getAndAdd(value.length)))
        addresses = addresses + (bytes -> address)
        address
    }
  }
}

class BasicIndexStore extends IndexStore {
  var values: Map[Array[Byte], Address] = Map()
  var comparator = BytesComparator

  def put(key: Array[Byte], value: Address) {
    values = values + (key -> value)
  }

  def get(key: Array[Byte]) = values.get(key)

  def flush() {}

  def query[A](start: Array[Byte], end: Array[Byte], reader:IndexReader[A]):A = {
    @tailrec
    def iter(values: Iterator[(Array[Byte], Address)], reader: IndexReader[A]): A =
      reader match {
        case IndexDone(a) => a
        case IndexMore(r) =>
          if (values.hasNext) {
            val next = values.next()
            if (comparator.between(next._1, start, end)) {
              val nextReader = r(Some(next))
              iter(values, nextReader)
            }
            else
              iter(values, reader)
          }
          else {
            val nextReader = r(None)
            iter(values, nextReader)
          }

      }
    iter(values.iterator, reader)
  }
}
