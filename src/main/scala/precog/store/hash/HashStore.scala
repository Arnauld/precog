package precog.store.hash

import precog.store.Address
import java.util.concurrent.atomic.AtomicInteger

/**
 * For more information see
 * <a href="http://en.wikibooks.org/wiki/Data_Structures/Hash_Tables">A Java implementation of extendible hashing</a>
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class HashStore[K,V](bucketStore:BucketStore,
                val hashFunction:(K)=>Long,
                rootAddress:Address,
                val pageSize:Int, // TODO think of moving the pageSz into the factory itself
                val pageFactory:PageFactory[K,V] = new MapBasedPageFactory[K,V]()) {

  // Debug stuffs
  var verbose = false
  var keyFormatter:(K) => String = (k:K) => String.valueOf(k)
  var hashFormatter:(Long) => String = (h:Long) => java.lang.Long.toBinaryString(h).reverse.padTo(26, "0").reverse.mkString
  var maskFormatter:(Long) => String = (m:Long) => java.lang.Long.toBinaryString(m).reverse.padTo(26, "0").reverse.mkString

  // Data stuffs
  var gd = 0
  var pages = Array(pageFactory.create(store = this, depth = 0))

  // Stats
  var rehash = 0
  var reallocTime = 0L
  var splitTime = 0L
  var preambuleTime = 0L
  var preambuleSplitTime = 0L
  var overallPutTime = 0L
  var stdPutTime = 0L

  private def getPage(h:Long):Page[K,V] = {
    val mask:Long = (1 << gd) - 1
    val idx = (h & mask).asInstanceOf[Int]

    if(verbose)
      println("Page " + idx + " found - matching key with " +
        "\n    hash: " + hashFormatter(h) +
        "\n    mask: " + hashFormatter(mask))
    pages(idx)
  }

  def put(key:K, v:V) {
    val preambuleBeg = System.nanoTime()
    val h = hashFunction(key)
    val page = getPage(h)

    if (full(page) && page.depth == gd) {
      pages = pages ++ pages
      gd = gd + 1
      if(verbose)
        println ("put# Page found is full, number of page is doubled (" + pages.size +  "), and bitmask depth increased by 1 (" + gd + ")")
    }
    val preambuleEnd = System.nanoTime()
    preambuleTime = preambuleTime + (preambuleEnd - preambuleBeg)

    if (full(page) && page.depth < gd) {
      val preambuleSplitBeg = System.nanoTime()
      if(verbose)
        println ("put# Page found is full, and is depth is lower (" + page.depth + ") than the greatest depth allowed (" + gd + ") rehashing will be triggered")

      rehash = rehash + 1

      page.put(h, key, v)
      val preambuleSplitEnd = System.nanoTime()
      preambuleSplitTime = preambuleSplitTime + (preambuleSplitEnd - preambuleSplitBeg)

      // split entries in two pages based on the new mask
      val splitBeg = System.nanoTime()
      val (p1,p2) = splitPage(page)
      val splitEnd = System.nanoTime()
      splitTime = splitTime + (splitEnd - splitBeg)

      if (verbose)
        println("put# Page has been splitted, reference will be adjusted")

      // reallocate page reference to the two new pages
      val reallocBeg = System.nanoTime()
      for(i <- 0 until pages.size if pages(i) == page) {
        if (((i >> page.depth) & 1) == 1) {
          if (verbose)
            println("  Page #" + i + " reallocated to higher page (2)")
          pages(i) = p2
        }
        else {
          if (verbose)
            println("  Page #" + i + " reallocated to lower page (1)")
          pages(i) = p1
        }
      }
      val reallocEnd = System.nanoTime()
      reallocTime = reallocTime + (reallocEnd - reallocBeg)
    }
    else {
      val stdPutBeg = System.nanoTime()
      page.put(h, key, v)
      val stdPutEnd = System.nanoTime()
      stdPutTime = stdPutTime + (stdPutEnd - stdPutBeg)
    }
    val overallPutEnd = System.nanoTime()
    overallPutTime = overallPutTime + (overallPutEnd - preambuleBeg)
  }

  def splitPage(page:Page[K,V]):(Page[K,V],Page[K,V]) = {
    val p1 = pageFactory.create(depth = page.depth + 1, store = this)
    val p2 = pageFactory.create(depth = page.depth + 1, store = this)
    val pageDepth = page.depth
    val pMask = ((1 << gd) - 1)
    page.foreach({(k,v) =>
      val h = hashFunction(k)
      val p = h & pMask
      val dst = if (((p >> pageDepth) & 1) == 1)
        p2
      else
        p1
      dst.put(h, k, v)
    })
    (p1,p2)
  }

  def get(k:K):Option[V] = {
    val h = hashFunction(k)
    getPage(h).get(h, k)
  }

  private def full(page:Page[K,V]):Boolean = (page.size >= pageSize)

  def numberOfDistinctPages = pages.map(_.id).distinct.length

}

object Page {
  private val generator = new AtomicInteger()
  def newId = generator.incrementAndGet()
}

trait PageFactory[K,V] {
  def create(store:HashStore[K,V],
             depth:Int):Page[K,V]
}

trait Page[K,V] {
  def id:Int
  def depth:Int
  def size:Int
  def put(hash:Long, key:K, value:V)
  def get(hash:Long, key:K):Option[V]

  // TODO remove me?
  def entries:TraversableOnce[(K,V)]

  // Traverse all entries in the Page
  // should be more efficient than `#entries`
  def foreach(f:(K,V) => Unit)

  override def equals(o: Any):Boolean =
    o.asInstanceOf[Page[K,V]].id == id
}

class MapBasedPageFactory[K,V] extends PageFactory[K,V] {
  def create(store: HashStore[K,V], depth: Int) =
    new MapBasedPage[K,V](depth)
}

class MapBasedPage[K,V](val depth:Int) extends Page[K,V] {
  var entries:scala.collection.Map[K,V] = Map.empty[K,V]
  val id = Page.newId

  def size = entries.size

  def put(hash:Long, key:K, value:V) {
    entries = entries + (key -> value)
  }

  def get(hash:Long, key:K):Option[V] = {
    entries.get(key)
  }

  def foreach(f:(K,V) => Unit) {
    entries.foreach( e => f(e._1, e._2))
  }
}

class ArrayBasedPageFactory[K,V] extends PageFactory[K,V] {

  def create(store: HashStore[K, V], depth: Int) =
    new ArrayBasedPage[K,V](store, depth)
}

class ArrayBasedPage[K,V](store: HashStore[K, V], val depth: Int) extends Page[K,V] {
  val id = Page.newId
  private var sz = 0
  private val values = new Array[(K,V,Long)](store.pageSize + 2)

  def entries:TraversableOnce[(K,V)] = values.slice(0, sz).map( x => (x._1, x._2) )

  def size = sz

  def put(hash:Long, key:K, value:V) {
    val idx = indexOf(hash, key)
    values(idx) = (key, value, hash)
    if (idx == sz)
      sz = sz + 1
  }

  private def indexOf(hash:Long, key:K) =
      0.until(sz).find({ i =>
        val v = values(i)
        (v._3 == hash && v._1 == key) }).getOrElse(sz)

  def get(hash:Long, key:K):Option[V] = {
    val idx = indexOf(hash, key)
    if (idx < sz)
      Some(values(idx)._2)
    else
      None
  }

  def foreach(f:(K,V) => Unit) {
    for(i <- 0 until sz) {
      val e = values(i)
      f(e._1, e._2)
    }
  }
}