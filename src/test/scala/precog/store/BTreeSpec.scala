package precog.store

;

import org.specs2.mutable.Specification
import precog.util.BytesComparator
import java.util.concurrent.atomic.AtomicLong
import util.Random

/**
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class BTreeSpec extends Specification {

  import precog.util.BytesImplicits._

  "BTree" should {

    "handle insert in an empty tree" in {
      val blockStore = mapBasedBlockStore

      val tree = new BTree(blockStore, BytesComparator, null, 4)
        .insert(bytes("aaa"), Address(15L))

      tree.find(bytes("aaa")) must_== Some(Address(15L))
    }

    "handle insert of at least its threshold elements" in {
      val blockStore = mapBasedBlockStore

      val tree = new BTree(blockStore, BytesComparator, null, 4)
        .insert(bytes("aaa"), Address(15L))
        .insert(bytes("ddd"), Address(45L))
        .insert(bytes("dda"), Address(55L))
        .insert(bytes("fff"), Address(35L))

      tree.find(bytes("aaa")) must_== Some(Address(15L))
      tree.find(bytes("ddd")) must_== Some(Address(45L))
      tree.find(bytes("dda")) must_== Some(Address(55L))
      tree.find(bytes("fff")) must_== Some(Address(35L))

      tree.traverseInOrder((key:Array[Byte], value:Address, arg:String) => {
        arg + "\n" + new String(key) + " > " + value
      }, "") must_==  """|
                        |aaa > Address(15)
                        |dda > Address(55)
                        |ddd > Address(45)
                        |fff > Address(35)""".stripMargin
    }

    "handle more entries than its node threshold" in {
      val blockStore = mapBasedBlockStore

      val tree = new BTree(blockStore, BytesComparator, null, 4)
                      .insert(bytes("aaa"), Address(15L))
                      .insert(bytes("ddd"), Address(45L))
                      .insert(bytes("dda"), Address(55L))
                      .insert(bytes("fff"), Address(35L))
                      .insert(bytes("eee"), Address(7L))

      tree.traverseInOrder((key:Array[Byte], value:Address, arg:String) => {
        arg + "\n" + new String(key) + " > " + value
      }, "") must_==  """|
                        |aaa > Address(15)
                        |dda > Address(55)
                        |ddd > Address(45)
                        |eee > Address(7)
                        |fff > Address(35)""".stripMargin
    }

    1.to(20).foreach { amount =>
      "support " + amount + " insert(s) in order" in {
        checkForANumerberOfInsert(amount = amount, blockSize = 4, shuffle = false)
      }
    }

    1.to(20).foreach { amount =>
      "support " + amount + " insert(s) in any order" in {
        checkForANumerberOfInsert(amount = amount, blockSize = 4, shuffle = true)
      }
    }

    "support a big amount of insert in any order (blocksize: 4)" in {
      checkForANumerberOfInsert(amount = 1000, blockSize = 4, shuffle = true)
    }

    "support a big amount of insert in any order (blocksize: 8)" in {
      checkForANumerberOfInsert(amount = 1000, blockSize = 8, shuffle = true)
    }

    def checkForANumerberOfInsert(amount:Int, blockSize:Int, shuffle:Boolean) {
      val empty:List[(String,Address)] = Nil
      val items = 1.to(amount).foldLeft((empty,"")) ( {(elems, index) =>
        val value = Address(index)
        val key = "0x" + (Integer.toHexString(index).reverse.padTo(4, "0").reverse).mkString
        val str = "\n" + key + " > Address(" + index + ")"
        ((key, value) :: elems._1, elems._2 + str)
      })

      val blockStore = mapBasedBlockStore
      val tree0 = new BTree(blockStore, BytesComparator, null, blockSize)
      val tree1 = (if(shuffle)
                    Random.shuffle(items._1)
                   else
                    items._1).foldLeft(tree0)({ (tree, t) =>
        tree.insert(bytes(t._1), t._2)
      })

      tree1.traverseInOrder((key:Array[Byte], value:Address, arg:String) => {
        arg + "\n" + new String(key) + " > " + value
      }, "") must_==  items._2
    }

    "replace a value when if the same key is reinserted" in {
      val blockStore = mapBasedBlockStore
      val tree = new BTree(blockStore, BytesComparator, null, 4)
        .insert(bytes("aaa"), Address(15L))
        .insert(bytes("ddd"), Address(45L))
        .insert(bytes("dda"), Address(55L))
        .insert(bytes("fff"), Address(35L))
        .insert(bytes("dda"), Address(78L))
        .insert(bytes("ddg"), Address(79L))
        .insert(bytes("ddh"), Address(71L))
        .insert(bytes("jjj"), Address(31L))

      tree.find(bytes("dda")) must_== Some(Address(78L))
    }
  }

  def mapBasedBlockStore = new BlockStore {
    val addrGen = new AtomicLong()
    var map:Map[Long, Node] = Map()
    def readBlock(address: Address) = map.get(address.offset).get

    def writeBlock(node: Node) = {
      val nAddr = addrGen.incrementAndGet()
      map = map + (nAddr -> node)
      Address(nAddr)
    }
  }

  def dump(tree: BTree, store: BlockStore) {
    dump(tree.rootAddress, store, 1)
  }

  def dump(addr:Address, store:BlockStore, indent:Int) {
    val rootBlock: Node = store.readBlock(addr)
    println("  ".*(indent) + addr + ": " + rootBlock)
    rootBlock match {
      case KVNode(pairs) =>
        // nothing
      case KPNode(pairs) =>
        pairs.foreach({ kp => dump(kp.address, store, indent + 1) })
    }
  }

}
