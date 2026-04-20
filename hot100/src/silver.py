# Databricks notebook source
# COMMAND ----------
# Install runtime dependencies
%pip install nrclex nltk langdetect

# COMMAND ----------
import pandas as pd
import re

from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, LongType, ArrayType
from pyspark.sql.functions import pandas_udf
from langdetect import detect
# COMMAND ----------
affect_keys = ['positive', 'negative', 'anger', 'anticipation', 'disgust', 'fear', 'joy', 'sadness', 'surprise', 'trust']

entry_schema = ArrayType(StructType([
    StructField('song', StringType()),
    StructField('artist', StringType()),
    StructField('this_week', IntegerType()),
    StructField('peak_position', IntegerType()),
    StructField('weeks_on_chart', IntegerType()),
    StructField('duration', LongType()),
    StructField('lyrics', StringType()),
    StructField('lastfm_listeners', StringType()),
    StructField('lastfm_playcount', StringType()),
    StructField('toptags', StructType([
        StructField('tag', ArrayType(StructType([
            StructField('name', StringType()),
            StructField('url', StringType()),
        ])))
    ])),
    StructField('summary', StringType()),
]))

nlp_schema = StructType(
    [StructField('language', StringType())]
    + [StructField('n_words', IntegerType())]
    + [StructField(k, FloatType()) for k in affect_keys]
)

@pandas_udf(nlp_schema)
def analyze_lyrics(lyrics: pd.Series) -> pd.DataFrame:
    """
    Basic analysis of song lyrics including cleaning 
    """

    # Need these imports inside the udf to avoid serialization issues
    from nltk import download
    from nltk.data import path as nltk_path
    from nrclex import NRCLex
    
    for corpus in ('stopwords', 'wordnet', 'punkt', 'punkt_tab'):
        download(corpus, download_dir='/tmp')
    nltk_path.append('/tmp')

    null_row = (None, None) + (None,) * len(affect_keys)
    results = []

    for lyric in lyrics:
        if lyric is None:
            results.append(null_row)
            continue

        # Basic cleaning + language detection
        lyric = re.sub(r'\[.*?\]\n', '', lyric)
        lyric = re.sub(r'\n\n', '\n', lyric)
        lyric = re.sub(r'[^ \nA-Za-z0-9$/]+', '', lyric)
        lyric = lyric.lower()

        try:
            lang = detect(lyric)
        except Exception:
            lang = None

        if lang != 'en':
            results.append((lang, None) + (None,) * len(affect_keys))
            continue

        # Basic tokenizing + get affect frequencies
        tokens = [w for w in lyric.replace('\n', ' ').split(' ') if w]
        n_words = len(tokens)
        nrc = NRCLex()
        nrc.load_raw_text(lyric)
        affect = nrc.affect_frequencies
        results.append(
            (lang, n_words) + tuple(affect.get(k, 0.0) for k in affect_keys)
        )

    return pd.DataFrame(results, columns=['language', 'n_words'] + affect_keys)

# COMMAND ----------
"""
Bronze -> Silver
1. Explode data json object into table
2. Basic cleaning and feature extraction (language, n_words, affect frequencies) of lyrics
"""
(spark.readStream
    .format('delta')
    .table('hot100.raw.bronze')
    .select(
        F.col('date'),
        F.col('_ingest_time'),
        F.col('_source_file'),
        F.explode(F.from_json(F.col('data'), entry_schema)).alias('entry'),
    )
    .select(
        F.col('date'),
        F.col('_ingest_time'),
        F.col('_source_file'),
        F.col('entry.song').alias('song'),
        F.col('entry.artist').alias('artist'),
        F.col('entry.this_week').alias('this_week'),
        F.col('entry.peak_position').alias('peak_position'),
        F.col('entry.weeks_on_chart').alias('weeks_on_chart'),
        F.col('entry.duration').alias('duration'),
        F.col('entry.lyrics').alias('lyrics'),
        F.col('entry.lastfm_listeners').alias('lastfm_listeners'),
        F.col('entry.lastfm_playcount').alias('lastfm_playcount'),
        F.col('entry.toptags').alias('toptags'),
        F.col('entry.summary').alias('summary'),
    )
    .withColumn('nlp', analyze_lyrics(F.col('lyrics')))
    .select(
        'date', 'song', 'artist', 'this_week', 'peak_position', 'weeks_on_chart', 'duration',
        'lastfm_listeners', 'lastfm_playcount', 'toptags', 'summary',
        '_ingest_time', '_source_file',
        F.col('nlp.language').alias('language'),
        F.col('nlp.n_words').alias('n_words'),
        *[F.col(f'nlp.{k}').alias(k) for k in affect_keys],
    )
    .writeStream
    .format('delta')
    .option('checkpointLocation', '/Volumes/hot100/processed/checkpoints/silver')
    .trigger(availableNow=True)
    .toTable('hot100.processed.silver')
)