package precog.store.hash

import org.specs2.mutable._
import precog.store.Address
import precog.util.{Bytes, Hash}

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class HashStoreSpec extends Specification {

  import precog.util.BytesImplicits._

  val PAGE_SZ = 16
  val skipLongRunning = true

  "HashStore" should {

    "handle insert in an empty set" in {
      val store = creatBucketStore
      val hash = new HashStore[Bytes, Address](store, Hash.Elf, null, PAGE_SZ)

      hash.put(Bytes("Ox34"), Address(34L))
      hash.get(Bytes("Ox34")) must_== Some(Address(34L))
    }

    1.to(PAGE_SZ).foreach { amount =>
      "support " + amount + " insert(s)" in {
        checkForANumerberOfInsert(amount = amount)
      }
    }

    1.to(10).foreach { amount =>
      "support an amount of insert bigger (x" + amount + ") than the page's size (" + amount * PAGE_SZ + " elements)" in {
        checkForANumerberOfInsert(amount = amount*PAGE_SZ)
      }
    }

    "support a big amount of insert: " + 2000 + " elements" in {
      checkForANumerberOfInsert(amount = 2000, pageSz = 24)
    }

    List(32, 64, 128, 256).foreach { pageSz =>
      "support an even bigger amount of insert: " + 100000 + " elements (pageSize: " + pageSz + ")" in {
        checkForANumerberOfInsert(amount = 100000, pageSz = pageSz, hash = Hash.Elf)
      }
    }

    List(32, 64, 128, 256).foreach { pageSz =>
      "support an even bigger amount of insert: " + 100000 + " elements (pageSize: " + pageSz + ") ArrayBasePAge" in {
        checkForANumerberOfInsert(amount = 100000,
          pageSz = pageSz,
          hash = Hash.Elf,
          pageFactory = arrayBasedFactory)
      }
    }
  }

  def arrayBasedFactory = new ArrayBasedPageFactory[Byte,Address]().asInstanceOf[PageFactory[Bytes,Address]]

  def checkForANumerberOfInsert(amount:Int,
                                pageSz:Int = PAGE_SZ,
                                hash:Hash = Hash.Elf,
                                pageFactory:PageFactory[Bytes,Address] = new MapBasedPageFactory[Bytes,Address]()) {
    val bucketStore = creatBucketStore
    val hashStore = new HashStore[Bytes, Address](bucketStore, hash, null, pageSz, pageFactory)

    for(index <- 1 to amount) {
      val e = entry(index)
      hashStore.put(Bytes(e._1), e._2)
    }

    // make sure one can now retrieves them
    for(index <- 1 to amount) {
      val e = entry(index)
      hashStore.get(Bytes(e._1)) must_== Some(e._2)
    }
  }

  def entry(index:Int):(String,Address) = {
    val value = Address(index)
    val key = "0x" + (Integer.toHexString(index).reverse.padTo(4, "0").reverse).mkString
    (key, value)
  }

  def creatBucketStore = new BucketStore {
    def writeBucket(node: Bucket) = null
    def readBucket(address: Address) = null
  }

}
