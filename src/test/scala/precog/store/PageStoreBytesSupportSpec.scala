package precog.store

import org.specs2.mutable.Specification
import precog.util.{BytesRandomAcess, Bytes}

/**
 * 
 * @author <a href="http://twitter.com/aloyer">@aloyer</a>
 */
class PageStoreBytesSupportSpec extends Specification {

  import precog.util.BytesImplicits._

  "PageStoreBytesSupport" should {
    "be sure that page can be reloaded in any order" in {
      val store = InMemoryPageStore()
      val rootAddress0 = store.writePage(PageType.IndexRootNode, Bytes("Eh hop"))
      val nodeAddress0 = store.writePage(PageType.IndexNode, Bytes("Rohhh lala"))
      val rootAddress1 = store.writePage(PageType.IndexRootNode, Bytes("Hank Moody rocks!"))
      val nodeAddress1 = store.writePage(PageType.IndexNode, Bytes("Hey dude!"))
      val nodeAddress2 = store.writePage(PageType.IndexNode, Bytes("Call me Bob"))

      val content: BytesRandomAcess = store.rawContent
      val storeReloaded:PageStore = InMemoryPageStore(content)

      val pageLoaded_na0: Page = storeReloaded.loadPage(nodeAddress0)
      pageLoaded_na0 must_== Page(PageType.IndexNode, Bytes("Rohhh lala"))

      val pageLoaded_na1: Page = storeReloaded.loadPage(nodeAddress1)
      pageLoaded_na1 must_== Page(PageType.IndexNode, Bytes("Hey dude!"))

      val pageLoaded_ra0: Page = storeReloaded.loadPage(rootAddress0)
      pageLoaded_ra0 must_== Page(PageType.IndexRootNode, Bytes("Eh hop"))

      val pageLoaded_na2: Page = storeReloaded.loadPage(nodeAddress2)
      pageLoaded_na2 must_== Page(PageType.IndexNode, Bytes("Call me Bob"))

      val pageLoaded_ra1: Page = storeReloaded.loadPage(rootAddress1)
      pageLoaded_ra1 must_== Page(PageType.IndexRootNode, Bytes("Hank Moody rocks!"))

    }

    "be sure that last page of a given type can be retrieved" in {
      val store = InMemoryPageStore()
      store.writePage(PageType.IndexRootNode, Bytes("Eh hop"))
      store.writePage(PageType.IndexNode, Bytes("Rohhh lala"))
      store.writePage(PageType.IndexRootNode, Bytes("Hank Moody rocks!"))
      store.writePage(PageType.IndexNode, Bytes("Hey dude!"))
      store.writePage(PageType.IndexNode, Bytes("Call me Bob"))

      val content: BytesRandomAcess = store.rawContent
      val storeReloaded:PageStore = InMemoryPageStore(content)

      var rootPage0 = storeReloaded.readLastPageOfType(PageType.IndexRootNode)
      rootPage0 must_== Some(Page(PageType.IndexRootNode, Bytes("Hank Moody rocks!")))
    }
  }

}
