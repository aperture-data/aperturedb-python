""" Extract text and images from various document formats. """

from bs4 import BeautifulSoup
from typing import Iterator, Optional, Union, Literal
import io
import pdfplumber
from .schema import Block, TextBlock, ImageBlock, FullTextBlock
import logging
from email.message import Message

logger = logging.getLogger(__name__)


class TextExtractor:
    def __init__(self, css_selector: Optional[str] = None, emit_full_text: bool = False):
        self.css_selector = css_selector
        self.emit_full_text = emit_full_text

    def _parse_content_type(self, content_type: str) -> (str, dict):
        """Parse the content type and return the main type and options.

        For example, if the content type is "text/plain; charset=UTF-8",
        this function will return ("text/plain", {"charset": "UTF-8"}).
        """
        email = Message()
        email["content-type"] = content_type
        params = email.get_params()
        return params[0][0], dict(params[1:])

    def extract_blocks(self, data: bytes, content_type: str) -> Iterator[Block]:
        full_text_buffer = io.StringIO() if self.emit_full_text else None
        mimetype, options = self._parse_content_type(content_type)
        # TODO: Handle options like charset
        if mimetype == "text/plain":
            gen = self._extract_plain_text_blocks(data)
        elif mimetype == "text/html":
            gen = self._extract_html_blocks(data)
        elif mimetype == "application/pdf":
            gen = self._extract_pdf_text_blocks(data)
        else:
            raise ValueError(f"Unsupported content type: {content_type}")
        title = None
        for block in gen:
            if self.emit_full_text and isinstance(block, TextBlock):
                if not title:
                    title = block.title
                full_text_buffer.write(block.text)
                full_text_buffer.write("\n\n")
            yield block
        if self.emit_full_text:
            yield FullTextBlock(text=full_text_buffer.getvalue(), title=title)

    def _extract_pdf_text_blocks(self, data: bytes) -> Iterator[Block]:
        with pdfplumber.open(io.BytesIO(data)) as pdf:
            title = pdf.metadata.get("Title")  # or None
            for page_number, page in enumerate(pdf.pages, start=1):
                text = page.extract_text()
                if text:
                    yield TextBlock(
                        text=text.strip(),
                        kind="body",
                        page_number=page_number,
                        title=title,
                    )

    def _extract_plain_text_blocks(self, data: bytes) -> Iterator[Block]:
        text = data.decode("utf-8", errors="replace")
        yield TextBlock(text=text)

    def _extract_html_blocks(self, data: bytes) -> Iterator[Block]:
        soup = BeautifulSoup(data, "html.parser")
        title_element = soup.find("title")
        title = title_element.get_text(strip=True) if title_element else None
        logger.info(f"Title found in HTML: {title}")

        content_tags = ["p", "h1", "h2", "h3", "h4", "h5", "li",
                        "figcaption", "img", "div", "span"]
        # TODO: Tag code blocks for language-sensitive splitting.
        # If a CSS selector is provided and matches, use its elements
        if self.css_selector:
            root_elements = soup.select(self.css_selector)
            if root_elements:
                for root in root_elements:
                    yield from self._yield_html_content_blocks(
                        self._filter_nested_elements(
                            root.find_all(content_tags)), title)
                return
            else:
                logger.warning(
                    f"No matches found for CSS selector: {self.css_selector}")

        # Fallback: no CSS selector provided or no matches found
        yield from self._yield_html_content_blocks(
            self._filter_nested_elements(soup.find_all(content_tags)), title)

    def _filter_nested_elements(self, elements):
        seen = set()
        filtered = 0
        for el in elements:
            if any(id(parent) in seen for parent in el.parents):
                filtered += 1
                continue
            seen.add(id(el))
            yield el

    def _yield_html_content_blocks(self, elements: list, title: Optional[str]) -> Iterator[Block]:
        pending_image = None
        current_anchor = None
        for el in elements:
            anchor = el.get("id") or el.get("name")
            if anchor:
                current_anchor = anchor
            if el.name == "img":
                image_url = el.get("src")
                alt_text = el.get("alt")
                if image_url:
                    if pending_image is not None and pending_image.has_text:
                        yield pending_image
                    # Defer image block in case caption is found; slightly hacky
                    pending_image = ImageBlock(
                        image_url=image_url, alt_text=alt_text, anchor=current_anchor, title=title)
            else:
                if el.name == "figcaption":
                    text = el.get_text(strip=True)
                    # Attach caption to last image if available
                    if pending_image is not None and text:
                        if pending_image.caption is None:
                            pending_image.caption = text
                        else:
                            logger.warning(
                                f"Multiple captions found for the same image: {current_anchor} - {pending_image} - {text}")

                text = el.get_text(strip=True)
                if text:
                    kind = self._tag_kind(el.name)
                    yield TextBlock(text=text, kind=kind, anchor=current_anchor, title=title)
            # TODO: Maybe get context text in other ways
        if pending_image is not None and pending_image.has_text:  # Flush final image block
            yield pending_image

    def _tag_kind(self, tag_name: str) -> Union[Literal["body"], Literal["title"], Literal["list"]]:
        """Map HTML tags to block kinds."""
        if tag_name in ["h1", "h2", "h3", "h4", "h5"]:
            return "title"
        elif tag_name == "li":
            return "list"
        else:
            return "body"
