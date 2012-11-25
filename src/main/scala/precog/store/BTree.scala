package precog.store

import precog.util.RichComparator
import annotation.tailrec

trait BlockStore {
  def readBlock(address: Address): Node
  def writeBlock(node:Node): Address
}

object BTree {
  val N = 4
}

/**
 * Inspired directly from:
 * <a href="https://github.com/Arnauld/mochusi/blob/master/src/mbtree.erl">mbtree.erl</a>
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class BTree(blockStore:BlockStore, comparator:RichComparator[Array[Byte]], val rootAddress:Address) {

  type K = Array[Byte]
  type V = Address

  var n:Int = 0 // Item count

  private def compare(k1: K, k2: K) = comparator.compare(k1, k2)

  /**
   * Retrieve a value based on its Key
   */
  def find(key:K):Option[V] = find(rootAddress, key)

  private def find(addr:Address, key:K):Option[V] =
    blockStore.readBlock(addr) match {
      case KPNode(pairs) =>
        traverseKPToFind(key, pairs)
      case KVNode(pairs) =>
        traverseKVToFind(key, pairs)
    }

  private def traverseKVToFind(key:K, pairs:List[KV]) : Option[V] =
    pairs match {
      case Nil => None
      case head::tail =>
        val cmp = compare(head.key, key)
        if (cmp < 0)
          traverseKVToFind(key, tail)
        else if (cmp > 0)
          None
        else // hurray!!!
          Some(head.value)
    }


  private def traverseKPToFind(key:K, pairs:List[KP]) : Option[V] =
    pairs match {
      case last::Nil => find(last.address, key)
      case head::tail =>
        val cmp = compare(head.key, key)
        if (cmp < 0)
          traverseKPToFind(key, tail)
        else
          find(head.address, key)
    }

  /**
   *
   */
  def insert(key:K, value:V):BTree = {
    assert(key!=null, "Key cannot be null")
    assert(value!=null, "Value cannot be null")

    insert_1(rootAddress, key, value, 0) match {
      case Ok(newAddr) =>
        new BTree(blockStore, comparator, newAddr)
      case Split(splitKey, left, right) =>
        val newNode   = KPNode(KP(splitKey, left), KP(null, right))
        new BTree(blockStore, comparator, blockStore.writeBlock(newNode))
    }
  }

  private def createKPNode(pairs: List[KP], depth:Int): InsertR = {
    //println("  ".*(depth) + "createKPNode: " + pairs + ")")
    if (pairs.size > BTree.N) {
      // node must be splitted
      val (left, head::rightTail) = pairs.splitAt(pairs.size / 2)
      val leftPairs = left ++ List(KP(null, head.address))
      val newLeftNode = KPNode(leftPairs)
      val newRightNode = KPNode(rightTail)
      val newLeftAddr = blockStore.writeBlock(newLeftNode)
      val newRightAddr = blockStore.writeBlock(newRightNode)
      val r = Split(head.key, newLeftAddr, newRightAddr)
      //println("  ".*(depth) + "createKPNode: => " + newLeftNode+ ", " + newRightNode + " ~"+ r)
      r
    }
    else {
      val node = KPNode(pairs)
      val addr = blockStore.writeBlock(node)
      val r = Ok(addr)
      //println("  ".*(depth) + "createKPNode: => " + node + " ~" + r)
      r
    }
  }

  private def createKVNode(pairs: List[KV], depth:Int): InsertR = {
    //println("  ".*(depth) + "createKVNode: " + pairs + ")")
    if (pairs.size > BTree.N) {
      // leaf node must be splitted
      val (leftPairs, rightPairs) = pairs.splitAt((pairs.size / 2) + 1)
      val newLeftNode = KVNode(leftPairs)
      val newRightNode = KVNode(rightPairs)
      val newLeftAddr = blockStore.writeBlock(newLeftNode)
      val newRightAddr = blockStore.writeBlock(newRightNode)
      val r = Split(rightPairs.head.key, newLeftAddr, newRightAddr)
      //println("  ".*(depth) + "createKVNode: => " + newLeftNode+ ", " + newRightNode + " ~"+ r)
      r
    }
    else {
      val node = KVNode(pairs)
      val addr = blockStore.writeBlock(node)
      val r = Ok(addr)
      //println("  ".*(depth) + "createKVNode: => " + node + " ~" + r)
      r
    }
  }

  private def insert_1(addr:Address, key:K, value:V, depth:Int):InsertR =
    if (addr == null) { // empty tree
      val node = KVNode(KV(key, value))
      val nodeAddr = blockStore.writeBlock(node)
      Ok(nodeAddr)
    }
    else {
      val block = blockStore.readBlock(addr)
      //println("  ".*(depth) + "insert_1(" + new String(key) + ", block: " + block + ")")
      block match {
        // non-leaf cases
        case KPNode(pairs) =>
          val a@(kp, reversedTaversed, tail) = traverseKPPairs(key, Nil, pairs)
          //println("  ".*(depth) + "insert_1: KP:case, " + a)

          val newPairs:List[KP] = insert_1(kp.address, key, value, depth + 1) match {
            case Ok(newAddr) =>
              //println("  ".*(depth) + "insert_1: Ok(" + newAddr + ")")
              //println("  ".*(depth) + "insert_1: " + reversedTaversed + " ::: " + tail)
              val reversed = (KP(kp.key, newAddr) :: reversedTaversed).reverse_:::(tail)
              //println("  ".*(depth) + "  >> " + reversed)
              reversed.reverse

            case Split(splitKey, left, right) =>
              //println("  ".*(depth) + "insert_1: Split(" + new String(splitKey) + ", left: " + left + ", right: " + right + ") ... ")
              //println("  ".*(depth) + "...  reversedTaversed: " + reversedTaversed + ", tail: " + tail)
              val newKP = KP(splitKey, left)
              // leaf child has been splitted: generate a new bucket
              tail match {
                case Nil => // KP is the last, simply discard it
                  val lastKP = KP(null, right)
                  val reversed = (newKP :: reversedTaversed).reverse_:::(List(lastKP))
                  reversed.reverse
                case last::Nil => // replace next addr
                  val newNext = KP(kp.key, right)
                  val reversed = (newNext :: newKP :: reversedTaversed).reverse_:::(tail)
                  reversed.reverse
                case next::tail1 => // replace next addr
                  val newNext = KP(kp.key, right)
                  val reversed = (newNext :: newKP :: reversedTaversed).reverse_:::(tail)
                  reversed.reverse
              }
          }
          createKPNode(newPairs, depth)

        // leaf cases
        case KVNode(pairs) =>
          val newPairs = insertPair(key, value, Nil, pairs)
          createKVNode(newPairs, depth)
      }
    }

  @tailrec
  private def insertPair(key:K, value:V, traversedInReversedOrder:List[KV], pairs:List[KV]): List[KV] = {
    pairs match {
      case Nil =>
        (KV(key, value) :: traversedInReversedOrder).reverse
      case head::tail =>
        val cmp = compare(head.key, key)
        if(cmp < 0)
          insertPair(key, value, head::traversedInReversedOrder, tail)
        else if(cmp > 0) {
          val reversed = (head :: KV(key, value) :: traversedInReversedOrder).reverse_:::(tail)
          reversed.reverse
        }
        else {
          val reversed = (KV(key, value) :: traversedInReversedOrder).reverse_:::(tail)
          reversed.reverse
        }

    }
  }

  @tailrec
  private def traverseKPPairs(key:K, traversedInReversedOrder:List[KP], pairs:List[KP]): (KP,List[KP],List[KP]) = {
     pairs match {
       case last::Nil =>
           (last, traversedInReversedOrder, Nil)
       case head::tail =>
         if(compare(head.key, key) < 0)
           traverseKPPairs(key, head::traversedInReversedOrder, tail)
         else
           (head, traversedInReversedOrder, tail)
       case Nil =>
         throw new IllegalStateException("Pairs must have at least one element to exist")
     }
  }

  def traverseInOrder[A](f:(K,V,A) => A, arg:A):A = traverseInOrder_addr(rootAddress, f, arg)

  private def traverseInOrder_addr[A](addr:Address, f:(K,V,A) => A, arg:A):A =
    blockStore.readBlock(addr) match {
      case KPNode(pairs) =>
        traverseInOrder_kp(pairs, f, arg)
      case KVNode(pairs) =>
        traverseInOrder_kv(pairs, f, arg)
    }

  private def traverseInOrder_kp[A](pairs:List[KP], f:(K,V,A) => A, arg:A):A =
    pairs match {
      case Nil  => arg
      case h::tail =>
        val arg1 = traverseInOrder_addr(h.address, f, arg)
        traverseInOrder_kp(tail, f, arg1)
    }

  private def traverseInOrder_kv[A](pairs:List[KV], f:(K,V,A) => A, arg:A):A =
    pairs match {
      case Nil  => arg
      case h::tail =>
        if(h.value == null)
          arg
        else {
          val arg1 = f(h.key, h.value, arg)
          traverseInOrder_kv(tail, f, arg1)
        }
    }

}

sealed trait InsertR
case class Ok(newNode:Address) extends InsertR
case class Split(splitKey:Array[Byte], left:Address, right:Address) extends InsertR

class DuplicateValueException extends Exception

trait Visitor {
  def visit(kp:KP)
  def visit(kp:KPNode)
  def visit(kp:KV)
  def visit(kp:KVNode)
}

sealed trait Node

case class KP(key:Array[Byte], address:Address) {
  override def toString = "KP(" + (if (key==null)
                                    "n/a"
                                  else
                                    new String(key)) + ", " + address + ")"
}
case class KPNode(pairs:List[KP]) extends Node {
  assert(pairs.head.key != null)
}
object KPNode {
  def apply(kp:KP):KPNode = KPNode(List(kp))
  def apply(kp1:KP, kp2:KP):KPNode = KPNode(List(kp1,kp2))
}

case class KV(key:Array[Byte], value:Address) {
  override def toString = "KV(" + new String(key) /*+ ", " + value */+ ")"
}
case class KVNode(pairs:List[KV]) extends Node {
  def this(kv:KV) = this(List(kv))
}
object KVNode {
  def apply(kv:KV):KVNode = KVNode(List(kv))
  def apply(kv1:KV, kv2:KV):KVNode = KVNode(List(kv1,kv2))
}

