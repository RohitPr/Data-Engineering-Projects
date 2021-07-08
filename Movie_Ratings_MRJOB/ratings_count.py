from mrjob.job import MRJob
from mrjob.step import MRStep


class Ratings(MRJob):
    def steps(self):
        return[
            MRStep(mapper=self.get_ratings,
                   reducer=self.count_ratings)
        ]

    def get_ratings(self, _, line):
        (userID, movieID, ratings, timestamp) = line.split('/')
        yield ratings, 1

    def count_ratings(self, key, values):
        yield key, sum(values)


if __name__ == "__main__":
    Ratings.run()
