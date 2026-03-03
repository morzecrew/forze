from typing import Literal

# ----------------------- #

IndexMode = Literal["fulltext", "phrase", "prefix", "exact"]
FieldType = Literal["text", "keyword"]
RankingStrategy = Literal["bm25", "tfidf", "native"]
