package precog.store.btree

import precog.store.Address

/**
 *
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
trait NodeStore {
  def readNode(address: Address): Node
  def writeNode(node:Node): Address
}
