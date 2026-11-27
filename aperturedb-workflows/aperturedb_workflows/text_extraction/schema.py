from typing import List, Optional, Union, Literal
from dataclasses import dataclass

import logging

logger = logging.getLogger(__name__)


@dataclass
class CrawlDocument:
    document_id: str
    url: str
    content_type: str
    blob: bytes


@dataclass
class TextBlock:
    """
    Represents a segment of readable text extracted from a document.

    'anchor' is included only if the original HTML element had an 'id' or 'name'.
    """
    text: str
    kind: Literal["body", "caption", "alt", "footnote"] = "body"
    anchor: Optional[str] = None
    # dom_path field removed
    page_number: Optional[int] = None
    # Optional title for the block, e.g., from a <title> tag
    title: Optional[str] = None

    def __str__(self):
        return self.text

    def url(self, url: str):
        if self.anchor:
            return f"{url}#{self.anchor}"
        elif self.page_number:
            return f"{url}#page={self.page_number}"
        return url

    def split(self, text: str):
        """Create new TextBlock with the same metadata but different text
        """
        return TextBlock(
            text=text,
            kind=self.kind,
            anchor=self.anchor,
            page_number=self.page_number,
            title=self.title
        )


@dataclass
class ImageBlock:
    """
    Represents an image extracted from HTML with optional caption or alt text.

    'anchor' is included only if the <img> tag has an 'id' or 'name' attribute.
    """
    image_url: str
    caption: Optional[str] = None
    alt_text: Optional[str] = None
    # dom_path field removed
    anchor: Optional[str] = None
    title: Optional[str] = None

    @property
    def best_text(self):
        return (self.caption or self.alt_text or "")

    def url(self, url: str):
        if self.anchor:
            return f"{url}#{self.anchor}"
        return url

    def __str__(self):
        return f"{self.best_text} ({self.image_url})"

    @property
    def has_text(self):
        return bool(self.best_text)


@dataclass
class FullTextBlock:
    """
    Represents the full extracted text.
    """
    text: str
    title: Optional[str] = None

    def __str__(self):
        return self.text


Block = Union[TextBlock, ImageBlock, FullTextBlock]


@dataclass
class Segment:
    text: str
    blocks: List[TextBlock]
    total_tokens: int
    title: Optional[str] = None

    def url(self, url: str) -> Optional[str]:
        """Pick the first URL from the blocks, if any
        """
        for block in self.blocks:
            new_url = block.url(url)
            if new_url and new_url != url:
                return new_url
        return url

    def page_number(self) -> Optional[int]:
        """Return the page number from the first block, if any
        """
        if not self.blocks:
            logger.warning(
                "Segment has no blocks, cannot determine page number.")
        for block in self.blocks:
            if block.page_number is not None:
                return block.page_number
        logger.warning("No blocks with page number found in segment.")
        return None

    @property
    def kinds(self) -> str:
        """Return kinds of blocks in this segment
        """
        return ",".join(sorted({block.kind for block in self.blocks}))
