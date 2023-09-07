from __future__ import annotations
from typing import Any

from aperturedb.Entities import Entities
from IPython.display import HTML, display
from aperturedb.NotebookHelpers import display_annotated_video
from ipywidgets import widgets


class Videos(Entities):
    """
    **The object mapper representation of videos in ApertureDB.**

    This class is a layer on top of the native query.
    It facilitates interactions with videos in the database in the pythonic way.
    """
    db_object = "_Video"

    def getitem(self, idx):
        item = super().getitem(idx)
        if self.blobs:
            if 'preview' not in item:
                item['preview'] = self.get_blob(item)
        return item

    def inspect(self, show_preview: bool = True, meta = None) -> Any:
        if meta == None:
            def meta(x): return []
        df = super().inspect()
        if show_preview == True:
            op = widgets.Output()
            with op:
                df['preview'] = df.apply(lambda x: display_annotated_video(
                    x["preview"], bboxes=meta(x)), axis=1)
                display(HTML(
                    "<div style='max-width: 100%; overflow: auto;'>" +
                    df.to_html(escape=False)
                    + "</div>"
                ))
            return op
        else:
            return df
