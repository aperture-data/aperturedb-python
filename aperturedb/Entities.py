from __future__ import annotations
import base64
from io import BytesIO
from typing import List
from aperturedb.Query import Query, EntityType

from aperturedb.Subscriptable import Subscriptable
from aperturedb.Constraints import Constraints
from aperturedb.Connector import Connector
from aperturedb.ParallelQuery import execute_batch
import pandas as pd
from PIL import Image
from ipywidgets import widgets
from IPython.display import display, HTML


class Entities(Subscriptable):
    db_object = "Entity"
    find_command = f"Find{db_object}"
    update_command = f"Update{db_object}"

    @classmethod
    def retrieve(cls,
                 db: Connector,
                 spec: Query
                 ) -> List[Entities]:
        cls.known_entities = load_entities_registry(
            custom_entities=spec.commands(v="with_class"))
        cls.db = db
        query = spec.query()
        print(f"query={query}")
        res, r, b = execute_batch(query, [], db, None)
        if res > 0:
            print(f"resp={r}")
        results = []
        for wc, req, resp in zip(spec.commands(v="with_class"), spec.commands(v="find_command"), r):
            try:
                # entities = known_entities[wc](resp[req]['entities'])
                entities = cls.known_entities[wc](
                    db=db, response=resp[req]['entities'], type=wc)
                # entities.response = resp[req]['entities']
                # print(resp[req]['entities'])
                # entities.pre_process()
                results.append(entities)
            except Exception as e:
                print(e)
                print(cls.known_entities)
                raise e
        return results

    def __init__(self, db: Connector = None, response: dict = None, type: str = None) -> None:
        super().__init__()
        self.db = db
        self.response = response
        self.type = type
        self.decorator = None
        self.get_image = False

    def pre_process(self) -> None:
        pass

    def getitem(self, idx):
        item = self.response[idx]
        if self.decorator is not None:
            for k, v in self.decorator(idx, self.adjacent).items():
                item[k] = v
        if self.get_image == True:
            buffer = self.get_image_by_index(idx)
            if buffer is not None:
                # nparr = np.frombuffer(buffer, dtype=np.uint8)
                item['thumbnail'] = Image.fromarray(
                    self.get_np_image_by_index(idx))
        return item

    def __len__(self):
        return len(self.response)

    def filter(self, predicate):
        return self.known_entities[self.type](db=self.db, response=list(filter(predicate, self.response)), type=self.type)

    def __add__(self, other: Entities) -> Entities:
        return Entities(response = self.response + other.response, type=self.type)

    def __sub__(self, other: Entities) -> Entities:
        return Entities(response = [x for x in self.response if x not in other.response], type=self.type)

    def sort(self, key) -> Entities:
        return Entities(response = sorted(self.response, key=key), type=self.type)

    def inspect(self, get_visual = False) -> pd.DataFrame:
        self.get_image = get_visual
        if not get_visual:
            return pd.json_normalize([item for item in self])
        else:
            # EXPERIMENTAL
            op = widgets.Output()

            def widget_interaction(c):
                def image_base64(im):
                    with BytesIO() as buffer:
                        im.save(buffer, 'jpeg')
                        return base64.b64encode(buffer.getvalue()).decode()

                def image_formatter(im):
                    return f'<img width={c["new"]} src="data:image/jpeg;base64,{image_base64(im)}">'

                with op:
                    op.clear_output()
                    display(HTML(self.inspect().to_html(
                        formatters={'thumbnail': image_formatter}, escape=False)))

            sizer = widgets.IntSlider(min=1, max=400, value=100)
            sizer.observe(widget_interaction, 'value')
            return sizer, op

    def update_properties(self, extra_properties: List[dict]) -> bool:
        for entity, properties in zip(self, extra_properties):
            query = [
                {
                    self.find_command: {
                        "_ref": 1,
                        "constraints": {
                            "_uniqueid": ["==", entity["_uniqueid"]]
                        },
                        "results": {
                            "blobs": False
                        }
                    }
                }, {
                    self.update_command: {
                        "ref": 1,
                        "properties": properties
                    }
                }
            ]
            res, r, b = execute_batch(query, [], self.db, None)
            print(r)
            return None

    def get_connected_entities(self, pk: str, type: EntityType, constraints: Constraints = None) -> List[Entities]:
        """
        Gets all entities adjacent to and clustered around items of the collection

        Args:
            pk (str): _description_
            type (EntityType): _description_
            constraints (Constraints, optional): _description_. Defaults to None.

        Returns:
            List[Entities]: _description_
        """
        result = []
        for entity in self:
            query = [
                {
                    self.find_command: {
                        "_ref": 1,
                        "unique": True,
                        "constraints": {
                            pk: ["==", int(entity[pk])]
                        }
                    }
                }, {
                    "FindEntity": {
                        "is_connected_to": {
                            "ref": 1
                        },
                        "with_class": type.value,
                        "constraints": constraints.constraints,
                        "results": {
                            "all_properties": True
                        }
                    }
                }
            ]
            res, r, b = execute_batch(query, [], self.db, None)

            result.append(self.known_entities[type.value](
                db=self.db, response=r[1]["FindEntity"]["entities"], type=type.value))
        return result


def load_entities_registry(custom_entities: List[str] = None) -> dict:
    from aperturedb.Polygons import Polygons
    from aperturedb.Images import Images

    known_entities = {
        EntityType.POLYGON.value: Polygons,
        EntityType.IMAGE.value: Images
    }
    for entity in set(custom_entities):
        if entity not in known_entities:
            known_entities[entity] = Entities
    return known_entities
