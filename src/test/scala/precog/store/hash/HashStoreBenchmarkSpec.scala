package precog.store.hash

import org.specs2.mutable._
import precog.store.Address
import precog.util.{Bytes, Hash}

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class HashStoreBenchmarkSpec extends Specification {

  import precog.util.BytesImplicits._

  val PAGE_SZ = 16
  val skipLongRunning = true
  val skipAllExcept = true

  "HashStore" should {

      "support an even bigger amount of insert: " + 500000 + " elements" in {
        checkForANumerberOfInsert(amount = 500000,
          pageSz = 128,
          hash = Hash.Elf,
          pageFactory = arrayBasedFactory)
      }
  }

  def arrayBasedFactory = new ArrayBasedPageFactory[Byte,Address]().asInstanceOf[PageFactory[Bytes,Address]]

  def checkForANumerberOfInsert(amount:Int,
                                pageSz:Int = PAGE_SZ,
                                hash:Hash = Hash.Elf,
                                verbose:Boolean = false,
                                pageFactory:PageFactory[Bytes,Address] = new MapBasedPageFactory[Bytes,Address]()) {
    val startTime = System.nanoTime()

    val bucketStore = creatBucketStore
    val hashStore = new HashStore[Bytes, Address](bucketStore, hash, null, pageSz, pageFactory)
    hashStore.verbose = verbose

    val buffer = new StringBuilder
    val insertTimeBeg = System.nanoTime()
    for(index <- 1 to amount) {
      val e = entry(index)
      hashStore.put(Bytes(e._1), e._2)

      if(verbose) {
        buffer.setLength(0)
        buffer.append("\n" + ("=".*(40)))
          .append("\n" + index + "/" + amount + "## Inserting: " + e)
          .append("\n" + ("-".*(40)))
        dumpHeader(hashStore, hash, buffer)
        dumpPages(hashStore, hash, buffer)
        println(buffer)
      }
    }
    val insertTimeEnd = System.nanoTime()

    // make sure one can now retrieves them
    for(index <- 1 to amount) {
      val e = entry(index)
      hashStore.get(Bytes(e._1)) must_== Some(e._2)
    }

    val endTime = System.nanoTime()
    buffer.setLength(0)
    buffer.append("Number of items inserted and checked: " + amount + ", page size: " + pageSz + "\n" +
            "Elapsed time: " + formatDurationNS(endTime - startTime) + " " +
            "(insert: " + formatDurationNS(insertTimeEnd - insertTimeBeg) +  ")")
    dumpHeader(hashStore, hash, buffer)
    println(buffer)
  }

  def dumpHeader(hashStore:HashStore[Bytes, Address], hash:Hash, out:StringBuilder) {
    out.append("\nNumber of rehash: " + hashStore.rehash +
      "\npreambuleTime.........: " + formatDurationNS(hashStore.preambuleTime) +
      "\npreambuleSplitTime....: " + formatDurationNS(hashStore.preambuleSplitTime) +
      "\nsplitTime.............: " + formatDurationNS(hashStore.splitTime) +
      "\nreallocTime...........: " + formatDurationNS(hashStore.reallocTime) +
      "\nstdPutTime............: " + formatDurationNS(hashStore.stdPutTime) +
      "\noverallPutTime........: " + formatDurationNS(hashStore.overallPutTime) +
      "\ngd: " + hashStore.gd +
      ", nbPages: " + hashStore.pages.size +
      " (" + hashStore.numberOfDistinctPages + ")" +
      " " + hashStore.pageFactory.getClass.getSimpleName +
      "\n")
  }

  def formatDurationNS(elapsed:Long) = (elapsed/1e6).asInstanceOf[Int] + "ms"

  def dumpPages(hashStore:HashStore[Bytes, Address], hash:Hash, out:StringBuilder) {
    for(i <- 0 until hashStore.pages.size) {
      val p = hashStore.pages(i)
      out.append("[" + i + " : " + formatBinary(i) + "] " +
        "depth:" + p.depth + ", #" + p.size + " " +
        "entries [\n  " + p.entries.map[String]({ x:(Bytes,Address) =>
          formatBinary(hash.hash(x._1.raw).asInstanceOf[Int]) + " " +
          new String(x._1.raw)
        }).mkString(",\n  ") + "\n]")
    }
  }

  def formatBinary(i:Int) = Integer.toBinaryString(i).reverse.padTo(4, "0").reverse.mkString

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
