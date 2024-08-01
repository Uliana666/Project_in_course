import requests
import pickle
import sys
import json
import dataclasses
from dataclasses import dataclass

data = []
ind = 0
k = 3000
cur = 0
num = 0
req_url = "http://localhost:1234/fanfic?start={}&count={}".format


@dataclass
class Part:
    offset: int
    length: int


@dataclass
class Metadata:
    url: str
    likes: int
    parts: list[Part]
    rating: str
    direction: str
    category: str


metadatas: list[Metadata] = []

added_fanfics, added_parts = 0, 0
skipped_fanfics, skipped_parts = 0, 0

with open("./data_small.raw", "w") as f:
    while len(result := requests.get(req_url(cur, k)).json()) != 0:
        cur += len(result)
        for fanfic in result:
            total_len = sum(map(len, fanfic['parts']))
            if total_len <= 7000:
                parts = []
                for part in fanfic['parts']:
                    start = f.tell()
                    f.write(part)
                    end = f.tell()
                    parts.append(Part(offset=start, length=end - start))
                meta = Metadata(url=fanfic['url'], likes=fanfic['likes'], parts=parts,
                                rating=fanfic['rating'], direction=fanfic['direction'],
                                category=fanfic['category'])
                metadatas.append(meta)

        print(num + 1, len(metadatas))
        num += 1


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


with open("./metadata_small.json", "w") as f:
    json.dump(metadatas, f, cls=EnhancedJSONEncoder)
