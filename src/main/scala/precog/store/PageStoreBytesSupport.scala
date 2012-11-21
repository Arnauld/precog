package precog.store

import precog.util.{BytesRandomAcess, Bytes}
import annotation.tailrec

trait PageStoreBytesSupport {
  def codeFor(pageType:PageType) = (pageType match {
    case IndexRootNode => 0
    case IndexNode => 1
    case BinaryRootNode => 2
    case BinaryNode => 3
  }).asInstanceOf[Byte]

  def pageTypeFor(code:Byte) = (code match {
    case 0 => IndexRootNode
    case 1 => IndexNode
    case 2 => BinaryRootNode
    case 3 => BinaryNode
  }).asInstanceOf[PageType]

  def sizeOfFooter = 1 /*page Type*/ + 8 /**/
  def sizeOfHeader = 1 /*page Type*/ + 8 /**/

  def loadFooterBeforeOffset(offset:Long, content:BytesRandomAcess) = {
    if (offset - sizeOfFooter < 0)
      throw new IndexOutOfBoundsException("OutOfBound offset: " + offset +" whereas footer size: " + sizeOfFooter)

    val footerBytes = new Array[Byte](sizeOfFooter)
    content.read((offset-sizeOfFooter).asInstanceOf[Int], sizeOfFooter, footerBytes)
    PageFooter(footerBytes(0), Bytes.readLong(1, footerBytes))
  }

  def writeFooter(content:BytesRandomAcess, pageType: PageType, pageStartOffset:Long) = {
    val footerBytes = new Array[Byte](sizeOfHeader)
    footerBytes(0) = codeFor(pageType)
    Bytes.writeLong(1, footerBytes, pageStartOffset)
    content.append(footerBytes)
  }

  def loadHeader(offset:Long, content:BytesRandomAcess) = {
    val headerBytes = new Array[Byte](sizeOfHeader)
    content.read(offset.asInstanceOf[Int], sizeOfHeader, headerBytes)
    PageHeader(headerBytes(0), Bytes.readLong(1, headerBytes))
  }

  def writeHeader(content:BytesRandomAcess, pageType: PageType, contentSize:Long) = {
    val headerBytes = new Array[Byte](sizeOfHeader)
    headerBytes(0) = codeFor(pageType)
    Bytes.writeLong(1, headerBytes, contentSize)
    content.append(headerBytes)
  }

  def loadPage(offset:Long, content:BytesRandomAcess):Page = {
    val header  = loadHeader(offset, content)
    val rawData = new Array[Byte](header.contentSize.asInstanceOf[Int])
    content.read((offset + sizeOfHeader).asInstanceOf[Int], header.contentSize.asInstanceOf[Int], rawData)
    Page(pageTypeFor(header.pageCode), Bytes(rawData))
  }


  def writePage(content:BytesRandomAcess, pageType: PageType, bytes: Bytes) = {
    val pageOffset = writeHeader(content, pageType, bytes.length())
    content.append(bytes.raw)
    writeFooter(content, pageType, pageOffset)
    pageOffset
  }

}

case class PageFooter(pageCode:Byte, pageStartOffset:Long)
case class PageHeader(pageCode:Byte, contentSize:Long)

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */

object InMemoryPageStore {
  def apply():PageStore = InMemoryPageStore(8*1024)
  def apply(capacity:Int):PageStore = new PageStoreBytesAdapter {
    val rawContent = BytesRandomAcess(new Array[Byte](capacity), 0)
    val initialOffset = 0L
  }
}

trait PageStoreBytesAdapter extends PageStore with PageStoreBytesSupport {
  def rawContent:BytesRandomAcess
  def initialOffset:Long

  private var offset = initialOffset

  override def loadPage(address: Address): Page = loadPage(address.offset, rawContent)

  override def readLastPageOfType(pageType: PageType):Option[Page] = {
    val searchedCode = codeFor(pageType)
    @tailrec def rewind(baseOffset:Long):Option[Page] = {
      // empty case
      if (baseOffset <= 0)
        None
      else {
        val footer = loadFooterBeforeOffset(baseOffset, rawContent)
        if (footer.pageCode == searchedCode)
          Some(loadPage(Address(footer.pageStartOffset)))
        else
          rewind(footer.pageStartOffset)
      }
    }
    rewind(offset)

  }

  override def writePage(pageType: PageType, raw: Bytes):Address = {
    val address = writePage(rawContent, pageType, raw)
    offset += sizeOfHeader + sizeOfFooter + raw.length()
    Address(address)
  }


}
