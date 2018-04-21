import os
import re
import logging
import time
import config

from operator import add

from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.broadcast import _broadcastRegistry

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : %(message)s',
    level=logging.INFO)

# Settings
datafile = config.phrase_generator['input-file']
output_file = config.phrase_generator['output-folder']
phrases_file = config.phrase_generator['phrase-file']
pre_tag = config.phrase_generator['dotag']
stopfile = config.phrase_generator['stop-file']


char_splitter = re.compile("[.,;!:()-]")
abspath = os.path.abspath(os.path.dirname(__file__))


def load_stop_words():

    return set(line.strip() for line in open(os.path.join(abspath, stopfile)))


def phrase_to_counts(phrases):
    """ strip any white space and send back a count of 1"""
    clean_phrases = []

    for p in phrases:
        word = p.strip()

        # we only need to count phrases, so ignore unigrams
        if len(word) > 1 and ' ' in word:
            clean_phrases.append([word, 1])

    return clean_phrases


def remove_special_characters(text):
    """remove characters that are not indicators of phrase boundaries"""
    return re.sub("([{}@\"$%&\\\/*'\"]|\d)", "", text)


def generate_candidate_phrases(text, stopwords):
    """ generate phrases using phrase boundary markers """

    # generate approximate phrases with punctation
    coarse_candidates = char_splitter.split(text.lower())

    candidate_phrases = []

    for coarse_phrase\
            in coarse_candidates:

        words = re.split("\\s+", coarse_phrase)
        previous_stop = False

        # examine each word to determine if it is a phrase boundary marker or
        # part of a phrase or lone ranger
        for w in words:

            if w in stopwords and not previous_stop:
                # phrase boundary encountered, so put a hard indicator
                candidate_phrases.append(";")
                previous_stop = True
            elif w not in stopwords and len(w) > 3:
                # keep adding words to list until a phrase boundary is detected
                candidate_phrases.append(w.strip())
                previous_stop = False

    # get a list of candidate phrases without boundary demarcation
    phrases = re.split(";+", ' '.join(candidate_phrases))

    return phrases


def generate_and_tag_phrases(text_rdd, min_phrase_count=50):
    """Find top phrases, tag corpora with those top phrases"""

    # load stop words for phrase boundary marking
    logging.info("Loading stop words...")
    stopwords = load_stop_words()

    # get top phrases with counts > 50
    logging.info("Generating and collecting top phrases...")
    top_phrases_rdd = \
        text_rdd.map(lambda txt: remove_special_characters(txt))\
        .map(lambda txt: generate_candidate_phrases(txt, stopwords)) \
        .flatMap(lambda phrases: phrase_to_counts(phrases)) \
        .reduceByKey(add) \
        .sortBy(lambda phrases: phrases[1], ascending=False) \
        .filter(lambda phrases: phrases[1] >= min_phrase_count) \
        .sortBy(lambda phrases: phrases[0], ascending=True) \
        .map(lambda phrases: (phrases[0], phrases[0].replace(" ", "_")))

    shortlisted_phrases = top_phrases_rdd.collectAsMap()
    logging.info("Done with phrase generation...")

    # write phrases to file which you can use down the road to tag your text
    logging.info("Saving top phrases to {0}".format(phrases_file))
    with open(os.path.join(abspath, phrases_file), "w") as f:
        for phrase in shortlisted_phrases:
            f.write(phrase)
            f.write("\n")

    # broadcast a few values so that these are not copied to the worker nodes
    # each time
    shortlisted_phrases_bc = sc.broadcast(shortlisted_phrases)
    keys = list(shortlisted_phrases.keys())
    keys.sort(key=len, reverse=True)
    sorted_key_bc = sc.broadcast(keys)  # sorts by descending length

    # tag corpora and save as new corpora
    logging.info("Tagging corpora with phrases...this will take a while")
    tagged_text_rdd = text_rdd.map(
        lambda txt: tag_data(
            txt,
            shortlisted_phrases_bc.value, sorted_key_bc.value))

    return tagged_text_rdd


def tag_data(original_text, phrase_transformation, keys):
    """Process the pipe separated file"""
    original_text = original_text.lower()

    # greedy approach, start with the longest phrase
    for phrase in keys:
        # keep track of all the substitutes for a given phrase
        original_text = original_text.replace(
            phrase, phrase_transformation[phrase])

    return original_text


if __name__ == "__main__":

    start = time.time()

    # Create a spark configuration with 20 threads.
    # This code will run locally on master
    conf = (SparkConf()
            . setAppName("sample app for reading files"))

    sc = SparkContext(conf=conf)

    # read text file, assumption here is that one document or sentences per line
    # if you have a json file or other formats to read, you will have to
    # change this a bit
    text_rdd = sc.textFile(os.path.join(abspath, datafile)).repartition(10)

    # generate candidate phrases and tag corpora with phrases
    tagged_rdd = generate_and_tag_phrases(
        text_rdd, min_phrase_count=config.phrase_generator['min-phrase-count'])

    # save data as a new corpora
    tagged_rdd.saveAsTextFile(
        output_file,
        "org.apache.hadoop.io.compress.GzipCodec")

    logging.info(
        "Done! You can find your phrases here {0} and tagged corpora here {1}".format(
            phrases_file, output_file))

    end = time.time()
    time_taken = round(((end - start) / 60), 2)
    print("Took {0} minutes to complete".format(time_taken))
