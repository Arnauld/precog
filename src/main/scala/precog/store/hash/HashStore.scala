package precog.store.hash

import precog.store.Address
import java.util.concurrent.atomic.AtomicInteger
import scala.collection

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
  var rehash = 0

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
    val h = hashFunction(key)
    val page = getPage(h)

    if (full(page) && page.depth == gd) {
      pages = pages ++ pages
      gd = gd + 1
      if(verbose)
        println ("put# Page found is full, number of page is doubled (" + pages.size +  "), and bitmask depth increased by 1 (" + gd + ")")
    }

    if (full(page) && page.depth < gd) {
      if(verbose)
        println ("put# Page found is full, and is depth is lower (" + page.depth + ") than the greatest depth allowed (" + gd + ") rehashing will be triggered")

      rehash = rehash + 1

      page.put(h, key, v)
      val (m1, m2) = splitEntries(page)

      val p1 = pageFactory.create(entries = m1, depth = page.depth + 1, store = this)
      val p2 = pageFactory.create(entries = m2, depth = page.depth + 1, store = this)

      if (verbose)
        println("put# Page has been splitted, reference will be adjusted")

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
    }
    else
      page.put(h, key, v)
  }

  private val useFunForSplit = false
  private def splitEntries(page:Page[K,V]) =  if(useFunForSplit)
                                                splitEntries_funWay(page)
                                              else
                                                splitEntries_mutableWay(page)

  private def splitEntries_mutableWay(page:Page[K,V]):(scala.collection.Map[K,V], scala.collection.Map[K,V]) = {
    val m1 = scala.collection.mutable.Map[K,V]()
    val m2 = scala.collection.mutable.Map[K,V]()

    for(e <- page.entries) {
      val k = e._1
      val h = hashFunction(k) & ((1 << gd) - 1)
      if (((h >> page.depth) & 1) == 1)
        m2 += (e._1 -> e._2) // Copy entry, need to check if Map.Entry reuse is also present in scala
      else
        m1 += (e._1 -> e._2)
    }

    (m1,m2)
  }

  private def splitEntries_funWay(page:Page[K,V]):(Map[K,V],Map[K,V])  = {
    page.entries.foldLeft((Map.empty[K,V], Map.empty[K,V]))({ (t, e) =>
      val k = e._1
      val h = hashFunction(k) & ((1 << gd) - 1)
      if ((h | (1 << page.depth)) == h)
        (t._1, t._2 + e)
      else
        (t._1 + e, t._2 )
    })
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
             depth:Int,
             entries:scala.collection.Map[K,V] = Map.empty[K,V]):Page[K,V]
}

trait Page[K,V] {
  def id:Int
  def depth:Int
  def size:Int
  def put(hash:Long, key:K, value:V)
  def get(hash:Long, key:K):Option[V]
  def entries:TraversableOnce[(K,V)]

  override def equals(o: Any):Boolean =
    o.isInstanceOf[Page[K,V]] && o.asInstanceOf[Page[K,V]].id == id
}

class MapBasedPageFactory[K,V] extends PageFactory[K,V] {
  def create(store: HashStore[K,V], depth: Int, entries: collection.Map[K, V]) =
    new MapBasedPage[K,V](entries, depth)
}

class MapBasedPage[K,V](var entries:scala.collection.Map[K,V], val depth:Int) extends Page[K,V] {
  val id = Page.newId

  def size = entries.size

  def put(hash:Long, key:K, value:V) {
    entries = entries + (key -> value)
  }

  def get(hash:Long, key:K):Option[V] = {
    entries.get(key)
  }
}

class ArrayBasedPageFactory[K,V] extends PageFactory[K,V] {

  def create(store: HashStore[K, V], depth: Int, entries: collection.Map[K, V]) =
    new ArrayBasedPage[K,V](store, depth, entries)
}

class ArrayBasedPage[K,V](store: HashStore[K, V], val depth: Int, initialEntries: collection.Map[K, V]) extends Page[K,V] {
  val id = Page.newId
  private var sz = 0
  private val values = new Array[(K,V,Long)](store.pageSize + 2)

  initialEntries.foreach({ t => values(sz) = (t._1, t._2, store.hashFunction(t._1))
    sz = sz + 1
  })

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
}