package precog.store

import org.specs2.mutable.Specification
import annotation.tailrec
import precog.util.{Bytes, BytesComparator}
import java.util.concurrent.atomic.AtomicLong
import scala.Array
import org.specs2.specification.Scope

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class DiskStoreStep1Spec extends Specification {

  import precog.util.BytesImplicits._

  "DiskStore" should {
    "be sure that Duplicate values cannot be stored more than once" in new basicStoreScope {
      /*
      That is, if you add 10000 keys all with the value "John Doe",
      then the text "John Doe" must be stored only a single time.
      */
      store.put("1", "John Doe")
      store.put("2", "John Doe")
      store.put("3", "John Doe")
      store.put(4,   "John Doe")
      store.put(5,   "John Doe")

      val soleEntry = (Bytes("John Doe") -> Address(0L))
      valueStore.addresses.toList must contain(soleEntry).only
    }

    "be sure that even Duplicate values can be retrieved" in new basicStoreScope {
      store.put("1", "John Doe")
      store.put("2", "John Doe")
      store.put("3", "John Doe")
      store.put(4,   "John Doe")
      store.put(5,   "John Doe")

      assertSomeBytes(store.get("1"), "John Doe")
      assertSomeBytes(store.get("2"), "John Doe")
      assertSomeBytes(store.get("3"), "John Doe")
      assertSomeBytes(store.get(4), "John Doe")
      assertSomeBytes(store.get(5), "John Doe")
    }

    def assertSomeBytes(actual:Option[Array[Byte]], expected:Array[Byte]) =
      actual.map(Bytes(_)) must_== Some(Bytes(expected))
  }

  trait basicStoreScope extends Scope {
    lazy val valueStore = new BasicBinaryStore()
    lazy val indexStorage = new BasicIndexStore()

    lazy val store = new DiskStoreAdapter(valueStore, indexStorage)
  }
}

class BasicBinaryStore extends BinaryStore {
  var addresses:Map[Bytes, Address] = Map()
  private val offset = new AtomicLong()

  override def flush() {}
  override def get(address: Address) = addresses.find(_._2 == address).map(_._1.raw)
  override def addressOf(bytes: Bytes) = {
    addresses.get(bytes) match {
      case Some(addr) => addr
      case None =>
        val address = Address(offset.getAndAdd(bytes.length()))
        addresses = addresses + (bytes -> address)
        address
    }
  }
}

class BasicIndexStore extends IndexStore {
  var values: Map[Bytes, Address] = Map()
  var comparator = BytesComparator

  override def put(key: Bytes, value: Address) {
    values = values + (key -> value)
  }

  override def get(key: Bytes) = values.get(key)

  override def flush() {}

  override def query[A](start: Bytes, end: Bytes, reader:IndexReader[A]):A = {
    @tailrec
    def iter(values: Iterator[(Bytes, Address)], reader: IndexReader[A]): A =
      reader match {
        case IndexReader.Done(a) => a
        case IndexReader.More(r) =>
          if (values.hasNext) {
            val next = values.next()
            // TODO comparator on bytes directly
            if (comparator.between(next._1.raw, start.raw, end.raw)) {
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
