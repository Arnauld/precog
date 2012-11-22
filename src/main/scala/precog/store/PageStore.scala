package precog.store

import precog.util.Bytes

/**
 *
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
trait PageStore {
  def readLastPageOfType(pageType:PageType):Option[Page]

  def writePage(pageType:PageType, raw:Bytes):Address

  def loadPage(address: Address): Page
}

sealed trait PageType
object PageType {
  case object IndexRootNode extends PageType
  case object IndexNode extends PageType
  case object BinaryRootNode extends PageType
  case object BinaryNode extends PageType
}

case class Page(pageType:PageType, raw:Bytes)
