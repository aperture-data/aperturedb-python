from __future__ import annotations
from typing import Dict, Any, List

from aperturedb.Connector import Connector
from aperturedb.Entities import Entities
from aperturedb.Query import Query
from IPython.display import HTML, display
from aperturedb.NotebookHelpers import display_annotated_video, TemporalBoundingBox
from ipywidgets import widgets


class Videos(Entities):
    db_object = "_Video"

    @classmethod
    def retrieve(cls,
                 db: Connector,
                 spec: Query,
                 with_adjacent: Dict[str, Query]) -> Videos:
        spec.with_class = cls.db_object

        videos = super().retrieve(
            db, spec=spec, with_adjacent=with_adjacent)[-1]

        return videos

    def getitem(self, idx):
        item = super().getitem(idx)
        if self.blobs:
            item['preview'] = self.get_blob(item)
        return item

    def inspect(self, show_preview: bool = True, meta = None) -> Any:
        if meta == None:
            def meta(x): return x
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
