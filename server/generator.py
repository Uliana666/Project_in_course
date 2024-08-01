import sys
import json
import dataclasses
from dataclasses import dataclass
from datasets import load_dataset
from tqdm import tqdm

dpath = sys.argv[1]
dataset = load_dataset(dpath, streaming=True, split="train")


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

with open("./data.raw", "w") as f:
    for fanfic in tqdm(dataset):
        if fanfic['likes'] < 20:
            skipped_fanfics += 1
            skipped_parts += len(fanfic['parts'])
            continue
        else:
            added_fanfics += 1
            added_parts += len(fanfic['parts'])
        parts = []
        for part in fanfic['parts']:
            start = f.tell()
            f.write(part['clean_text'])
            end = f.tell()
            parts.append(Part(offset=start, length=end-start))
        meta = Metadata(url=fanfic['url'], likes=fanfic['likes'], parts=parts,
                        rating=fanfic['rating'], direction=fanfic['direction'],
                        category=fanfic['category'])
        metadatas.append(meta)


print(f"Added: fanfics: {added_fanfics}, parts: {added_parts}")
print(f"Skipped: fanfics: {skipped_fanfics}, parts: {skipped_parts}")


class EnhancedJSONEncoder(json.JSONEncoder):
    def default(self, o):
        if dataclasses.is_dataclass(o):
            return dataclasses.asdict(o)
        return super().default(o)


with open("./metadata.json", "w") as f:
    json.dump(metadatas, f, cls=EnhancedJSONEncoder)
