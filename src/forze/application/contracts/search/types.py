from typing import Literal

# ----------------------- #

SearchIndexMode = Literal["fulltext", "phrase", "prefix", "exact"]
SearchFieldType = Literal["text", "keyword"]
SearchRankingStrategy = Literal["bm25", "tfidf", "native"]
