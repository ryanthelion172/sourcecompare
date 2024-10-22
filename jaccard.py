from mrjob.job import MRJob
from mrjob.step import MRStep
import contractions
import json
import itertools
import nltk
from nltk.corpus import stopwords
import re
import logging

nltk.download('stopwords')
sw = stopwords.words('english')

class MRSimilarityJob(MRJob):
    
    # First Mapper: Processes article text, removes stopwords, and emits URL and cleaned text.
    def mapper_first(self, _, line):
        source = ""
        articles = json.loads(line)
        for article in articles:
            try:
                if "verge" in article["page"]:
                    source = "theverge"
                else:
                    source = cnbc
            except:
                source = "ksl"
            text = article['content'].lower().strip()
            tokens = contractions.fix(text).split()
            cleaned_tokens = [re.sub(r'[^\w\s]', ' ', token).strip() for token in tokens if re.sub(r'[^\w\s]', '', token)]
            compacted_tokens = [word for word in cleaned_tokens if word.strip()]
            filtered_tokens = [word for word in compacted_tokens if word not in sw]
            text = ' '.join(filtered_tokens)
            yield source, text

    # First Reducer: Just forwards the data (pass-through)
    def reducer_first(self, url, text):
        for t in text:
            yield None, f"{url}:::{t}"

    # Second Mapper: Combines articles and generates pairs for similarity comparison.
    def mapper_second(self, _, lines):
        # Input format: <url>:::<content>
        for line in lines:
            line = line.strip()
            if not line:
                continue

            combs = itertools.combinations(lines, 2)
            for comb in combs:
                if comb[0] != line or not comb[1] or comb[0] == '\t':
                    continue
                pairing = {'A': comb[0], 'B': comb[1]}
                yield None, json.dumps(pairing)

    # Second Reducer: Calculates Jaccard similarity and outputs the result.
    def reducer_second(self, _, pairings):
        def jaccard(A, B):
            return len(A.intersection(B)) / len(A.union(B))
        for pairing_str in pairings:
            pairing = json.loads(pairing_str)
            A = set(pairing['A'])
            B = set(pairing['B'])
            pairing['similarity'] = jaccard(A, B)
            yield json.dumps(pairing), None

    # Steps definition for the job.
    def steps(self):
        return [
            MRStep(mapper=self.mapper_first, reducer=self.reducer_first),
            MRStep(mapper=self.mapper_second, reducer=self.reducer_second)
        ]

if __name__ == '__main__':
    MRSimilarityJob.run()

